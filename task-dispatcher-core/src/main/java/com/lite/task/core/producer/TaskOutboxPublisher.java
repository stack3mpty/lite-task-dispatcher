package com.lite.task.core.producer;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.util.TraceIdHolder;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.domain.task.entity.TaskOutboxEvent;
import com.lite.task.domain.task.event.TaskCreatedEvent;
import com.lite.task.infrastructure.kafka.producer.TaskEventProducer;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.persistence.repository.TaskOutboxEventRepository;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.DistributedLock;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Outbox publisher for reliable task dispatch.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskOutboxPublisher {

    private final TaskOutboxEventRepository taskOutboxEventRepository;
    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskCacheOperator taskCacheOperator;
    private final TaskQueueOperator taskQueueOperator;
    private final DelayQueueOperator delayQueueOperator;
    private final TaskEventProducer taskEventProducer;
    private final DistributedLock distributedLock;

    @Value("${task.outbox.enabled:true}")
    private boolean enabled;

    @Value("${task.outbox.batch-size:100}")
    private int batchSize;

    @Value("${task.outbox.max-retries:10}")
    private int maxRetries;

    @Value("${task.outbox.base-retry-delay-ms:2000}")
    private long baseRetryDelayMs;

    public void publishAfterCommit(Long outboxId) {
        if (!enabled || outboxId == null) {
            return;
        }
        publishSingle(outboxId);
    }

    @Scheduled(fixedDelayString = "${task.outbox.scan-interval-ms:2000}")
    @Transactional
    public void recoverUnpublishedEvents() {
        if (!enabled) {
            return;
        }

        String lockKey = "outbox:scan";
        if (!distributedLock.tryLock(lockKey, 1, 15)) {
            return;
        }
        try {
            List<TaskOutboxEvent> dueEvents = taskOutboxEventRepository.findDueEvents(
                    List.of(TaskOutboxEvent.STATUS_NEW, TaskOutboxEvent.STATUS_RETRY),
                    LocalDateTime.now(),
                    PageRequest.of(0, batchSize)
            );
            for (TaskOutboxEvent event : dueEvents) {
                publishSingle(event.getId());
            }
        } finally {
            distributedLock.unlock(lockKey);
        }
    }

    @Transactional
    public void publishSingle(Long outboxId) {
        if (outboxId == null) {
            return;
        }

        TaskOutboxEvent outboxEvent = taskOutboxEventRepository.findById(outboxId).orElse(null);
        if (outboxEvent == null || !outboxEvent.isPublishable()) {
            return;
        }

        String lockKey = "outbox:task:" + outboxEvent.getTaskId();
        if (!distributedLock.tryLock(lockKey, 1, 30)) {
            return;
        }
        try {
            doPublish(outboxEvent);
        } finally {
            distributedLock.unlock(lockKey);
        }
    }

    private void doPublish(TaskOutboxEvent outboxEvent) {
        TraceIdHolder.bindTraceId(null);
        TraceIdHolder.bindTaskId(outboxEvent.getTaskId());
        try {
            TaskInstance task = taskInstanceRepository.findByTaskId(outboxEvent.getTaskId()).orElse(null);
            if (task == null) {
                outboxEvent.markRetry("Task not found for outbox event", maxRetries, baseRetryDelayMs);
                taskOutboxEventRepository.save(outboxEvent);
                return;
            }
            if (task.getStatus() != TaskStatus.PENDING
                    && task.getStatus() != TaskStatus.RETRYING) {
                outboxEvent.markPublished();
                taskOutboxEventRepository.save(outboxEvent);
                log.debug("Skip outbox publish for non-dispatchable task: taskId={}, status={}",
                        task.getTaskId(), task.getStatus());
                return;
            }

            // Ensure cache is available before enqueueing so worker can always load task details.
            taskCacheOperator.save(task);

            if (task.isDelayed()) {
                delayQueueOperator.add(task.getTaskId(), task.getExecuteAt());
            } else {
                TaskPriority priority = task.getPriorityEnum() != null ? task.getPriorityEnum() : TaskPriority.DEFAULT;
                taskQueueOperator.push(task.getTaskId(), priority);
            }

            taskEventProducer.publishTaskCreated(new TaskCreatedEvent(
                    task.getTaskId(),
                    task.getTaskType(),
                    task.getPriority(),
                    task.getParams(),
                    task.getCreatedBy()
            ));

            outboxEvent.markPublished();
            taskOutboxEventRepository.save(outboxEvent);
            log.debug("Outbox event published successfully: outboxId={}, taskId={}",
                    outboxEvent.getId(), outboxEvent.getTaskId());
        } catch (Exception e) {
            outboxEvent.markRetry(e.getMessage(), maxRetries, baseRetryDelayMs);
            taskOutboxEventRepository.save(outboxEvent);
            log.error("Failed to publish outbox event: outboxId={}, taskId={}, status={}, retryCount={}, error={}",
                    outboxEvent.getId(),
                    outboxEvent.getTaskId(),
                    outboxEvent.getStatus(),
                    outboxEvent.getRetryCount(),
                    e.getMessage(),
                    e);
        } finally {
            TraceIdHolder.clearTaskId();
            TraceIdHolder.clearTraceId();
        }
    }
}
