package com.lite.task.core.scheduler;

import com.lite.task.common.enums.TaskStatus;
import com.lite.task.core.executor.retry.ExponentialBackoffRetry;
import com.lite.task.core.executor.retry.RetryPolicy;
import com.lite.task.domain.task.entity.TaskDefinition;
import com.lite.task.domain.task.entity.TaskExecutionLog;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.domain.task.event.TaskFailedEvent;
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
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Timeout scanner for running tasks.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TimeoutTaskScheduler {

    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskDefinitionRepository taskDefinitionRepository;
    private final TaskExecutionLogRepository taskExecutionLogRepository;
    private final TaskCacheOperator taskCacheOperator;
    private final TaskQueueOperator taskQueueOperator;
    private final DelayQueueOperator delayQueueOperator;
    private final TaskEventProducer taskEventProducer;
    private final DistributedLock distributedLock;

    @Value("${task.scheduler.timeout.enabled:true}")
    private boolean enabled;

    @Value("${task.scheduler.timeout.batch-size:200}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${task.scheduler.timeout.interval-ms:5000}")
    @Transactional
    public void scanTimedOutRunningTasks() {
        if (!enabled) {
            return;
        }

        List<TaskInstance> runningTasks = taskInstanceRepository.findByStatus(
                TaskStatus.RUNNING, PageRequest.of(0, batchSize)
        ).getContent();
        if (runningTasks.isEmpty()) {
            return;
        }

        LocalDateTime now = LocalDateTime.now();
        for (TaskInstance running : runningTasks) {
            if (running.getStartedAt() == null) {
                continue;
            }
            TaskDefinition definition = taskDefinitionRepository.findById(running.getTaskDefId()).orElse(null);
            int timeoutSeconds = definition != null && definition.getTimeoutSeconds() != null
                    ? definition.getTimeoutSeconds()
                    : 60;

            if (running.getStartedAt().plusSeconds(timeoutSeconds).isAfter(now)) {
                continue;
            }
            handleTimeout(running.getTaskId(), timeoutSeconds, now, definition);
        }
    }

    private void handleTimeout(String taskId, int timeoutSeconds, LocalDateTime now, TaskDefinition definition) {
        String executeLockKey = "execute:" + taskId;
        if (!distributedLock.tryLockTask(executeLockKey, 30)) {
            return;
        }

        try {
            TaskInstance task = taskInstanceRepository.findByTaskId(taskId).orElse(null);
            if (task == null || task.getStatus() != TaskStatus.RUNNING || task.getStartedAt() == null) {
                return;
            }
            if (task.getStartedAt().plusSeconds(timeoutSeconds).isAfter(now)) {
                return;
            }

            long durationMs = Duration.between(task.getStartedAt(), now).toMillis();
            int attemptNumber = task.getRetryCount() + 1;
            String timeoutMessage = "Task execution timeout";

            task.fail(timeoutMessage);

            boolean willRetry = task.scheduleRetry();
            if (willRetry) {
                RetryPolicy retryPolicy = ExponentialBackoffRetry.fromConfig(
                        task.getMaxRetry() != null ? task.getMaxRetry() : 3,
                        definition != null ? definition.getInitialRetryDelay() : 1000L,
                        definition != null ? definition.getRetryMultiplier() : 2.0,
                        definition != null ? definition.getMaxRetryDelay() : 60000L
                );
                long delayMs = retryPolicy.getNextDelay(task.getRetryCount());

                taskCacheOperator.updateRetry(taskId, task.getRetryCount(), TaskStatus.RETRYING);
                delayQueueOperator.addWithDelay(taskId, delayMs);
                taskEventProducer.publishTaskFailed(TaskFailedEvent.withRetry(
                        task.getTaskId(),
                        task.getTaskType(),
                        timeoutMessage,
                        "Exceeded timeout: " + timeoutSeconds + "s",
                        durationMs,
                        "timeout-scheduler",
                        attemptNumber
                ));
            } else {
                taskCacheOperator.updateOnFailure(taskId, timeoutMessage, TaskStatus.DEAD);
                taskEventProducer.publishTaskFailed(TaskFailedEvent.dead(
                        task.getTaskId(),
                        task.getTaskType(),
                        timeoutMessage,
                        "Exceeded timeout: " + timeoutSeconds + "s",
                        durationMs,
                        "timeout-scheduler",
                        attemptNumber
                ));
            }

            taskQueueOperator.clearRunning(taskId);
            taskInstanceRepository.save(task);
            taskExecutionLogRepository.save(TaskExecutionLog.timeout(
                    task,
                    attemptNumber,
                    durationMs,
                    "timeout-scheduler",
                    Objects.toString(task.getExecutorId(), "unknown")
            ));

            log.warn("Timeout handled: taskId={}, status={}, retryCount={}",
                    taskId, task.getStatus(), task.getRetryCount());
        } catch (Exception e) {
            log.error("Failed to handle timeout task: taskId={}, error={}", taskId, e.getMessage(), e);
        } finally {
            distributedLock.unlockTask(executeLockKey);
        }
    }
}
