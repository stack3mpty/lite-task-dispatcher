package com.lite.task.core.dispatcher;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.exception.ErrorCode;
import com.lite.task.common.exception.TaskException;
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
import com.lite.task.infrastructure.persistence.repository.TaskExecutionLogRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.DistributedLock;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
    private final TaskExecutionLogRepository executionLogRepository;
    private final TaskQueueOperator taskQueueOperator;
    private final DelayQueueOperator delayQueueOperator;
    private final TaskCacheOperator taskCacheOperator;
    private final DistributedLock distributedLock;
    private final TaskEventProducer eventProducer;
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
    @Async("taskExecutor")
    public boolean executeTask(String taskId) {
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
                    .build();

            // Update Redis status to RUNNING
            taskCacheOperator.updateOnStart(taskId, executorId);

            // Mark as running in Redis (for timeout tracking)
            taskQueueOperator.markRunning(taskId, executorId, definition.getTimeoutSeconds());

            // Publish started event
            eventProducer.publishTaskStarted(new TaskStartedEvent(
                    taskId, task.getTaskType(), executorId, context.getAttemptNumber()));

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
        }
    }

    /**
     * Handle successful execution
     */
    private void handleSuccess(TaskInstance task, TaskResult result, long duration, int attemptNumber) {
        // Update Redis status (primary storage)
        taskCacheOperator.updateOnComplete(task.getTaskId());

        // Save execution log to DB (result is stored here, not in Redis)
        TaskExecutionLog logEntry = TaskExecutionLog.success(
                task, attemptNumber, duration, executorIp, executorId);
        logEntry.setResult(result.getData()); // Store result in execution log
        asyncPersistExecutionLog(logEntry);

        // Async update task instance in DB (for record keeping)
        task.complete(result.getData());
        asyncPersistTask(task);

        // Publish event
        eventProducer.publishTaskCompleted(new TaskCompletedEvent(
                task.getTaskId(), task.getTaskType(), result.getData(),
                duration, executorId, attemptNumber));

        log.info("Task completed successfully: taskId={}, duration={}ms", task.getTaskId(), duration);
    }

    /**
     * Handle failed execution
     */
    private void handleFailure(TaskInstance task, TaskDefinition definition,
                               TaskResult result, long duration, int attemptNumber) {
        task.fail(result.getMessage());

        // Save execution log to DB
        TaskExecutionLog logEntry = TaskExecutionLog.failure(
                task, attemptNumber, result.getMessage(), result.getErrorDetail(),
                duration, executorIp, executorId);
        asyncPersistExecutionLog(logEntry);

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
            } else {
                // Max retries exceeded, mark as DEAD
                taskCacheOperator.updateOnFailure(task.getTaskId(), result.getMessage(), TaskStatus.DEAD);
            }
        } else {
            // Non-retryable failure
            taskCacheOperator.updateOnFailure(task.getTaskId(), result.getMessage(), TaskStatus.FAILED);
        }

        // Async update task instance in DB
        asyncPersistTask(task);

        // Publish event
        TaskFailedEvent event = willRetry ?
                TaskFailedEvent.withRetry(task.getTaskId(), task.getTaskType(),
                        result.getMessage(), result.getErrorDetail(), duration, executorId, attemptNumber) :
                task.getStatus() == TaskStatus.DEAD ?
                        TaskFailedEvent.dead(task.getTaskId(), task.getTaskType(),
                                result.getMessage(), result.getErrorDetail(), duration, executorId, attemptNumber) :
                        TaskFailedEvent.permanent(task.getTaskId(), task.getTaskType(),
                                result.getMessage(), result.getErrorDetail(), duration, executorId, attemptNumber);

        eventProducer.publishTaskFailed(event);

        log.info("Task failed: taskId={}, willRetry={}, status={}",
                task.getTaskId(), willRetry, task.getStatus());
    }

    /**
     * Handle execution error
     */
    private void handleExecutionError(String taskId, Exception e) {
        try {
            // Update Redis status
            taskCacheOperator.updateOnFailure(taskId, e.getMessage(), TaskStatus.FAILED);
        } catch (Exception ex) {
            log.error("Error handling execution error: taskId={}, error={}", taskId, ex.getMessage());
        }
    }

    /**
     * Async persist task to database (for record keeping)
     */
    @Async
    public void asyncPersistTask(TaskInstance task) {
        try {
            taskInstanceRepository.save(task);
            log.debug("Task persisted to database: taskId={}", task.getTaskId());
        } catch (Exception e) {
            log.error("Failed to persist task to database (non-blocking): taskId={}, error={}",
                    task.getTaskId(), e.getMessage());
        }
    }

    /**
     * Async persist execution log to database
     */
    @Async
    public void asyncPersistExecutionLog(TaskExecutionLog logEntry) {
        try {
            executionLogRepository.save(logEntry);
            log.debug("Execution log persisted: taskId={}", logEntry.getTaskId());
        } catch (Exception e) {
            log.error("Failed to persist execution log (non-blocking): taskId={}, error={}",
                    logEntry.getTaskId(), e.getMessage());
        }
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
}
