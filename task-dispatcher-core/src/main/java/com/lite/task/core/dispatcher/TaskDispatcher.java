package com.lite.task.core.dispatcher;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.exception.ErrorCode;
import com.lite.task.common.exception.TaskException;
import com.lite.task.common.util.TraceIdHolder;
import com.lite.task.core.executor.TaskExecutor;
import com.lite.task.core.executor.TaskResult;
import com.lite.task.core.executor.retry.ExponentialBackoffRetry;
import com.lite.task.core.executor.retry.RetryPolicy;
import com.lite.task.domain.task.entity.TaskDefinition;
import com.lite.task.domain.task.entity.TaskExecutionLog;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.domain.task.event.TaskCompletedEvent;
import com.lite.task.domain.task.event.TaskFailedEvent;
import com.lite.task.domain.task.event.TaskStartedEvent;
import com.lite.task.domain.task.valueobject.TaskContext;
import com.lite.task.infrastructure.kafka.producer.TaskEventProducer;
import com.lite.task.infrastructure.persistence.repository.TaskDefinitionRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.DistributedLock;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Task Dispatcher Service
 *
 * Responsible for dispatching and executing tasks
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskDispatcher {

    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskDefinitionRepository taskDefinitionRepository;
    private final TaskQueueOperator taskQueueOperator;
    private final DelayQueueOperator delayQueueOperator;
    private final TaskCacheOperator taskCacheOperator;
    private final DistributedLock distributedLock;
    private final TaskEventProducer eventProducer;
    private final TaskPersistenceAsyncService taskPersistenceAsyncService;
    private final MeterRegistry meterRegistry;
    private final List<TaskExecutor> executors;

    private final Map<String, TaskExecutor> executorMap = new ConcurrentHashMap<>();

    @Value("${task.dispatcher.executor-id:#{T(java.util.UUID).randomUUID().toString()}}")
    private String executorId;

    private String executorIp;

    /**
     * Initialize executor map
     */
    @jakarta.annotation.PostConstruct
    public void init() {
        // Register executors
        for (TaskExecutor executor : executors) {
            executorMap.put(executor.getType().toUpperCase(), executor);
            log.info("Registered executor: type={}, class={}", executor.getType(), executor.getClass().getName());
        }

        // Get executor IP
        try {
            executorIp = InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            executorIp = "unknown";
        }
    }

    /**
     * Poll and execute next task
     *
     * @return true if a task was executed
     */
    public boolean pollAndExecute() {
        // Try to get task from priority queues
        String taskId = taskQueueOperator.popByPriority();
        if (taskId == null) {
            return false;
        }

        return executeTask(taskId);
    }

    /**
     * Poll and execute next task with specific priority
     */
    public boolean pollAndExecute(TaskPriority priority) {
        String taskId = taskQueueOperator.pop(priority);
        if (taskId == null) {
            return false;
        }

        return executeTask(taskId);
    }

    /**
     * Execute a specific task
     */
    public boolean executeTask(String taskId) {
        TraceIdHolder.bindTraceId(null);
        TraceIdHolder.bindTaskId(taskId);

        // Acquire distributed lock to prevent duplicate execution
        String lockKey = "execute:" + taskId;
        if (!distributedLock.tryLockTask(lockKey, 300)) {
            log.warn("Failed to acquire lock for task: {}", lockKey);
            return false;
        }

        try {
            // Load task from Redis Hash (primary storage)
            TaskInstance task = taskCacheOperator.get(taskId);

            if (task == null) {
                log.warn("Task not found in cache: {}", taskId);
                return false;
            }

            // Check status
            if (task.getStatus() != TaskStatus.PENDING && task.getStatus() != TaskStatus.RETRYING) {
                log.warn("Task not in executable status: taskId={}, status={}", taskId, task.getStatus());
                return false;
            }

            LocalDateTime dbStartTime = LocalDateTime.now();
            int claimed = taskInstanceRepository.claimForExecution(taskId, executorId, dbStartTime);
            if (claimed <= 0) {
                log.warn("Task claim failed, skip execution: taskId={}", taskId);
                return false;
            }

            // Load task definition
            TaskDefinition definition = taskDefinitionRepository.findById(task.getTaskDefId())
                    .orElseThrow(() -> new TaskException(ErrorCode.TASK_TYPE_NOT_FOUND,
                            "Task definition not found: " + task.getTaskDefId()));

            // Get executor
            TaskExecutor executor = executorMap.get(definition.getExecutorType().toUpperCase());
            if (executor == null) {
                throw TaskException.executorNotFound(definition.getExecutorType());
            }

            // Build context
            TaskContext context = TaskContext.builder()
                    .taskId(taskId)
                    .taskType(task.getTaskType())
                    .executorType(definition.getExecutorType())
                    .params(task.getParams())
                    .executorConfig(definition.getExecutorConfig())
                    .attemptNumber(task.getRetryCount() + 1)
                    .maxAttempts(task.getMaxRetry())
                    .timeoutSeconds(definition.getTimeoutSeconds())
                    .callbackUrl(task.getCallbackUrl())
                    .traceId(TraceIdHolder.getOrCreateTraceId())
                    .build();

            // Update Redis status to RUNNING
            taskCacheOperator.updateOnStart(taskId, executorId);

            // Keep domain object state in sync with cache state transition
            task.start(executorId);

            // Mark as running in Redis (for timeout tracking)
            taskQueueOperator.markRunning(taskId, executorId, definition.getTimeoutSeconds());

            // Publish started event
            TaskStartedEvent startedEvent = new TaskStartedEvent(
                    taskId, task.getTaskType(), executorId, context.getAttemptNumber());
            startedEvent.setTraceId(context.getTraceId());
            eventProducer.publishTaskStarted(startedEvent);

            // Execute
            long startTime = System.currentTimeMillis();
            TaskResult result = executor.execute(context);
            long duration = System.currentTimeMillis() - startTime;

            // Handle result
            if (result.isSuccess()) {
                handleSuccess(task, result, duration, context.getAttemptNumber());
            } else {
                handleFailure(task, definition, result, duration, context.getAttemptNumber());
            }

            return true;

        } catch (Exception e) {
            log.error("Error executing task: taskId={}, error={}", taskId, e.getMessage(), e);
            handleExecutionError(taskId, e);
            return false;
        } finally {
            distributedLock.unlockTask(lockKey);
            taskQueueOperator.clearRunning(taskId);
            TraceIdHolder.clearTaskId();
            TraceIdHolder.clearTraceId();
        }
    }

    /**
     * Handle successful execution
     */
    private void handleSuccess(TaskInstance task, TaskResult result, long duration, int attemptNumber) {
        Map<String, Object> resultData = result.getData();

        task.complete(resultData);

        // Update Redis status (primary storage)
        taskCacheOperator.updateOnComplete(task.getTaskId());

        // Ensure DB identity for consistent task status update and execution log
        ensureTaskIdentity(task);

        // Save execution log to DB (result is stored here, not in Redis)
        if (task.getId() != null) {
            TaskExecutionLog logEntry = TaskExecutionLog.success(
                    task, attemptNumber, duration, executorIp, executorId);
            logEntry.setResult(resultData); // Store result in execution log
            taskPersistenceAsyncService.persistExecutionLog(logEntry);
        } else {
            log.warn("Skip execution log persistence because task identity is missing: taskId={}", task.getTaskId());
        }

        // Async update task instance in DB (for record keeping)
        taskPersistenceAsyncService.persistTask(task);

        // Publish event
        TaskCompletedEvent event = new TaskCompletedEvent(
                task.getTaskId(), task.getTaskType(), result.getData(),
                duration, executorId, attemptNumber);
        event.setTraceId(TraceIdHolder.getOrCreateTraceId());
        eventProducer.publishTaskCompleted(event);

        publishCallbackIfNeeded(task, task.getStatus().getCode(), resultData, null, attemptNumber);
        recordExecutionMetrics(task.getTaskType(), "SUCCESS", duration);

        log.info("Task completed successfully: taskId={}, duration={}ms", task.getTaskId(), duration);
    }

    /**
     * Handle failed execution
     */
    private void handleFailure(TaskInstance task, TaskDefinition definition,
                               TaskResult result, long duration, int attemptNumber) {
        task.fail(result.getMessage());

        // Ensure DB identity for consistent task status update and execution log
        ensureTaskIdentity(task);

        // Save execution log to DB
        if (task.getId() != null) {
            TaskExecutionLog logEntry = TaskExecutionLog.failure(
                    task, attemptNumber, result.getMessage(), result.getErrorDetail(),
                    duration, executorIp, executorId);
            taskPersistenceAsyncService.persistExecutionLog(logEntry);
        } else {
            log.warn("Skip execution log persistence because task identity is missing: taskId={}", task.getTaskId());
        }

        // Check if should retry
        boolean willRetry = false;
        if (result.isRetryable()) {
            willRetry = task.scheduleRetry();
            if (willRetry) {
                // Calculate retry delay
                RetryPolicy retryPolicy = ExponentialBackoffRetry.fromConfig(
                        definition.getMaxRetryAttempts(),
                        definition.getInitialRetryDelay(),
                        definition.getRetryMultiplier(),
                        definition.getMaxRetryDelay()
                );
                long delay = retryPolicy.getNextDelay(task.getRetryCount());

                // Update Redis for retry
                taskCacheOperator.updateRetry(task.getTaskId(), task.getRetryCount(), TaskStatus.RETRYING);

                // Add to delay queue for retry
                delayQueueOperator.addWithDelay(task.getTaskId(), delay);
                log.info("Task scheduled for retry: taskId={}, attempt={}, delay={}ms",
                        task.getTaskId(), task.getRetryCount(), delay);
                meterRegistry.counter("task.retry.total", "taskType", safeTagValue(task.getTaskType())).increment();
            } else {
                // Max retries exceeded, mark as DEAD
                taskCacheOperator.updateOnFailure(task.getTaskId(), result.getMessage(), TaskStatus.DEAD);
                meterRegistry.counter("task.dead.total", "taskType", safeTagValue(task.getTaskType())).increment();
            }
        } else {
            // Non-retryable failure
            taskCacheOperator.updateOnFailure(task.getTaskId(), result.getMessage(), TaskStatus.FAILED);
        }

        // Async update task instance in DB
        taskPersistenceAsyncService.persistTask(task);

        // Publish event
        TaskFailedEvent event = willRetry ?
                TaskFailedEvent.withRetry(task.getTaskId(), task.getTaskType(),
                        result.getMessage(), result.getErrorDetail(), duration, executorId, attemptNumber) :
                task.getStatus() == TaskStatus.DEAD ?
                        TaskFailedEvent.dead(task.getTaskId(), task.getTaskType(),
                                result.getMessage(), result.getErrorDetail(), duration, executorId, attemptNumber) :
                        TaskFailedEvent.permanent(task.getTaskId(), task.getTaskType(),
                                result.getMessage(), result.getErrorDetail(), duration, executorId, attemptNumber);
        event.setTraceId(TraceIdHolder.getOrCreateTraceId());

        eventProducer.publishTaskFailed(event);
        if (task.getStatus().isTerminal()) {
            publishCallbackIfNeeded(task, task.getStatus().getCode(), null, result.getMessage(), attemptNumber);
        }
        recordExecutionMetrics(task.getTaskType(), task.getStatus().name(), duration);

        log.info("Task failed: taskId={}, willRetry={}, status={}",
                task.getTaskId(), willRetry, task.getStatus());
    }

    /**
     * Handle execution error
     */
    private void handleExecutionError(String taskId, Exception e) {
        String errorMessage = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();

        try {
            // Update Redis status
            taskCacheOperator.updateOnFailure(taskId, errorMessage, TaskStatus.FAILED);
        } catch (Exception ex) {
            log.error("Error handling execution error: taskId={}, error={}", taskId, ex.getMessage());
        }

        try {
            TaskInstance task = taskCacheOperator.get(taskId);
            if (task == null) {
                return;
            }

            if (!task.getStatus().isTerminal()) {
                task.setStatus(TaskStatus.FAILED);
                task.setErrorMessage(errorMessage);
                task.setFinishedAt(LocalDateTime.now());
                task.setUpdatedAt(LocalDateTime.now());
            }

            ensureTaskIdentity(task);
            taskPersistenceAsyncService.persistTask(task);

            if (task.getId() != null) {
                TaskExecutionLog logEntry = TaskExecutionLog.failure(
                        task,
                        task.getRetryCount() + 1,
                        errorMessage,
                        e.toString(),
                        0L,
                        executorIp,
                        executorId
                );
                taskPersistenceAsyncService.persistExecutionLog(logEntry);
            }
        } catch (Exception ex) {
            log.error("Failed to sync execution error to DB: taskId={}, error={}", taskId, ex.getMessage(), ex);
        }

        meterRegistry.counter("task.execution.total",
                "taskType", "unknown",
                "status", "FAILED").increment();
    }

    /**
     * Get executor by type
     */
    public TaskExecutor getExecutor(String executorType) {
        return executorMap.get(executorType.toUpperCase());
    }

    /**
     * Check if executor exists
     */
    public boolean hasExecutor(String executorType) {
        return executorMap.containsKey(executorType.toUpperCase());
    }

    private void ensureTaskIdentity(TaskInstance task) {
        if (task.getId() != null) {
            return;
        }

        taskInstanceRepository.findByTaskId(task.getTaskId()).ifPresent(existing -> task.setId(existing.getId()));
    }

    private void publishCallbackIfNeeded(TaskInstance task,
                                         String status,
                                         Map<String, Object> result,
                                         String errorMessage,
                                         int attemptNumber) {
        if (task.getCallbackUrl() == null || task.getCallbackUrl().isBlank()) {
            return;
        }

        Map<String, Object> callbackPayload = new ConcurrentHashMap<>();
        callbackPayload.put("taskId", task.getTaskId());
        callbackPayload.put("taskType", task.getTaskType());
        callbackPayload.put("status", status);
        callbackPayload.put("callbackUrl", task.getCallbackUrl());
        callbackPayload.put("result", result);
        callbackPayload.put("errorMessage", errorMessage);
        callbackPayload.put("attemptNumber", attemptNumber);
        callbackPayload.put("traceId", TraceIdHolder.getOrCreateTraceId());
        callbackPayload.put("executorId", executorId);
        eventProducer.publishCallback(task.getTaskId(), callbackPayload);
    }

    private void recordExecutionMetrics(String taskType, String status, long durationMs) {
        String safeTaskType = safeTagValue(taskType);
        String safeStatus = safeTagValue(status);
        meterRegistry.counter("task.execution.total", "taskType", safeTaskType, "status", safeStatus).increment();
        Timer.builder("task.execution.duration")
                .description("Task execution duration")
                .tags("taskType", safeTaskType, "status", safeStatus)
                .register(meterRegistry)
                .record(Math.max(durationMs, 0L), TimeUnit.MILLISECONDS);
    }

    private String safeTagValue(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
