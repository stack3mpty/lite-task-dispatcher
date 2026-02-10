package com.lite.task.core.producer;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.exception.ErrorCode;
import com.lite.task.common.exception.TaskException;
import com.lite.task.common.util.Assert;
import com.lite.task.common.util.IdGenerator;
import com.lite.task.domain.task.entity.TaskDefinition;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.domain.task.entity.TaskOutboxEvent;
import com.lite.task.infrastructure.persistence.repository.TaskDefinitionRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.persistence.repository.TaskOutboxEventRepository;
import com.lite.task.infrastructure.redis.DeduplicationChecker;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.RateLimiter;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Task Producer Service
 *
 * Responsible for creating and submitting tasks
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskProducer {

    private final TaskDefinitionRepository taskDefinitionRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskQueueOperator taskQueueOperator;
    private final DelayQueueOperator delayQueueOperator;
    private final TaskCacheOperator taskCacheOperator;
    private final RateLimiter rateLimiter;
    private final DeduplicationChecker deduplicationChecker;
    private final TaskOutboxEventRepository taskOutboxEventRepository;
    private final TaskOutboxPublisher taskOutboxPublisher;

    /**
     * Submit a new task
     *
     * @param taskType    Task type
     * @param params      Task parameters
     * @param priority    Task priority (0-4)
     * @param executeAt   Scheduled execution time (null for immediate)
     * @param callbackUrl Callback URL (optional)
     * @param createdBy   Creator identifier
     * @return Created task instance
     */
    @Transactional(rollbackFor = Exception.class)
    public TaskInstance submit(String taskType, Map<String, Object> params,
                               int priority, LocalDateTime executeAt,
                               String callbackUrl, String createdBy) {
        // 1. Validate task type
        TaskDefinition definition = taskDefinitionRepository.findByTaskType(taskType)
                .orElseThrow(() -> new TaskException(ErrorCode.TASK_TYPE_NOT_FOUND,
                        "Task type not found: " + taskType));

        Assert.isTrue(definition.isEnabled(), ErrorCode.TASK_TYPE_NOT_FOUND,
                "Task type is disabled: " + taskType);

        // 2. Check rate limit
        if (!rateLimiter.tryAcquire(taskType, definition.getRateLimit(), definition.getRateLimit())) {
            throw new TaskException(ErrorCode.RATE_LIMIT_EXCEEDED,
                    "Rate limit exceeded for task type: " + taskType);
        }

        // 3. Check deduplication (if params provided)
        if (params != null && !params.isEmpty()) {
            if (!deduplicationChecker.checkAndMark(taskType, params, Duration.ofHours(24))) {
                throw new TaskException(ErrorCode.TASK_DUPLICATE,
                        "Duplicate task detected for type: " + taskType);
            }
        }

        // 4. Create task instance
        String taskId = IdGenerator.generateIdStr();
        TaskPriority taskPriority = TaskPriority.fromLevel(priority);
        LocalDateTime now = LocalDateTime.now();

        TaskInstance task = TaskInstance.builder()
                .taskId(taskId)
                .taskDefId(definition.getId())
                .taskType(taskType)
                .status(TaskStatus.PENDING)
                .priority(priority)
                .params(params)
                .maxRetry(definition.getMaxRetryAttempts())
                .executeAt(executeAt != null ? executeAt : now)
                .callbackUrl(callbackUrl)
                .createdBy(createdBy)
                .createdAt(now)
                .updatedAt(now)
                .build();

        // 5. Persist task and outbox intent in the same local transaction.
        // External publishing to Redis/Kafka happens after commit via outbox.
        TaskInstance persistedTask = taskInstanceRepository.save(task);
        TaskOutboxEvent outboxEvent = taskOutboxEventRepository.save(buildOutboxEvent(persistedTask));

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    taskOutboxPublisher.publishAfterCommit(outboxEvent.getId());
                }
            });
        } else {
            taskOutboxPublisher.publishAfterCommit(outboxEvent.getId());
        }

        log.info("Task accepted: taskId={}, priority={}, delayed={}, outboxId={}",
                taskId,
                taskPriority,
                executeAt != null && executeAt.isAfter(LocalDateTime.now()),
                outboxEvent.getId());
        return persistedTask;
    }

    /**
     * Submit task with default priority
     */
    public TaskInstance submit(String taskType, Map<String, Object> params) {
        return submit(taskType, params, TaskPriority.DEFAULT.getLevel(), null, null, null);
    }

    /**
     * Submit task with priority
     */
    public TaskInstance submit(String taskType, Map<String, Object> params, int priority) {
        return submit(taskType, params, priority, null, null, null);
    }

    /**
     * Submit delayed task
     */
    public TaskInstance submitDelayed(String taskType, Map<String, Object> params,
                                      Duration delay, String createdBy) {
        LocalDateTime executeAt = LocalDateTime.now().plus(delay);
        return submit(taskType, params, TaskPriority.DEFAULT.getLevel(), executeAt, null, createdBy);
    }

    /**
     * Submit task with callback
     */
    public TaskInstance submitWithCallback(String taskType, Map<String, Object> params,
                                           String callbackUrl, String createdBy) {
        return submit(taskType, params, TaskPriority.DEFAULT.getLevel(), null, callbackUrl, createdBy);
    }

    /**
     * Cancel a pending task
     */
    public boolean cancel(String taskId) {
        // Get task from Redis (primary storage)
        TaskInstance task = taskCacheOperator.get(taskId);

        // Fallback to database if not in Redis
        if (task == null) {
            task = taskInstanceRepository.findByTaskId(taskId)
                    .orElseThrow(() -> TaskException.notFound(taskId));
        }

        if (!task.getStatus().isCancellable()) {
            throw TaskException.invalidStatus(taskId, task.getStatus().getCode(), "PENDING or CREATED");
        }

        // Remove from queue
        TaskPriority priority = task.getPriorityEnum();
        boolean removed = taskQueueOperator.remove(taskId, priority);

        if (!removed) {
            // Try to remove from delay queue
            removed = delayQueueOperator.remove(taskId);
        }

        // Update Redis status
        taskCacheOperator.updateStatus(taskId, TaskStatus.CANCELLED);

        // Update database
        task.cancel();
        taskInstanceRepository.save(task);

        // Remove deduplication mark so task can be resubmitted
        if (task.getParams() != null) {
            deduplicationChecker.remove(task.getTaskType(), task.getParams());
        }

        log.info("Task cancelled: taskId={}", taskId);
        return true;
    }

    private TaskOutboxEvent buildOutboxEvent(TaskInstance task) {
        Map<String, Object> payload = new HashMap<>();
        payload.put("taskId", task.getTaskId());
        payload.put("taskType", task.getTaskType());
        payload.put("priority", task.getPriority());
        payload.put("executeAt", task.getExecuteAt() != null ? task.getExecuteAt().toString() : null);
        payload.put("createdBy", task.getCreatedBy());

        return TaskOutboxEvent.builder()
                .eventType(TaskOutboxEvent.EVENT_TASK_SUBMITTED)
                .taskId(task.getTaskId())
                .payload(payload)
                .status(TaskOutboxEvent.STATUS_NEW)
                .build();
    }

}
