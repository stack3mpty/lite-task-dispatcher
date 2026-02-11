package com.lite.task.core.scheduler;

import com.lite.task.infrastructure.persistence.repository.TaskExecutionLogRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.DistributedLock;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Scheduled cleanup job for historical task data.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DataCleanupScheduler {

    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskExecutionLogRepository taskExecutionLogRepository;
    private final DistributedLock distributedLock;
    private final MeterRegistry meterRegistry;

    @Value("${task.scheduler.cleanup.enabled:true}")
    private boolean enabled;

    @Value("${task.scheduler.cleanup.retention-days.task-instance:30}")
    private int taskInstanceRetentionDays;

    @Value("${task.scheduler.cleanup.retention-days.execution-log:30}")
    private int executionLogRetentionDays;

    @Scheduled(fixedDelayString = "${task.scheduler.cleanup.interval-ms:3600000}")
    @Transactional
    public void cleanupHistory() {
        if (!enabled) {
            return;
        }

        String lockKey = "task:cleanup:history";
        if (!distributedLock.tryLock(lockKey, 1, 300)) {
            return;
        }

        try {
            LocalDateTime taskBefore = LocalDateTime.now().minusDays(Math.max(taskInstanceRetentionDays, 1));
            LocalDateTime logBefore = LocalDateTime.now().minusDays(Math.max(executionLogRetentionDays, 1));

            int deletedTasks = taskInstanceRepository.deleteOldCompletedTasks(taskBefore);
            int deletedLogs = taskExecutionLogRepository.deleteOldLogs(logBefore);

            meterRegistry.counter("task.cleanup.deleted.total", "entity", "task_instance").increment(deletedTasks);
            meterRegistry.counter("task.cleanup.deleted.total", "entity", "task_execution_log").increment(deletedLogs);

            log.info("Data cleanup completed: deletedTasks={}, deletedLogs={}, taskBefore={}, logBefore={}",
                    deletedTasks, deletedLogs, taskBefore, logBefore);
        } catch (Exception e) {
            log.error("Data cleanup failed: {}", e.getMessage(), e);
        } finally {
            distributedLock.unlock(lockKey);
        }
    }
}
