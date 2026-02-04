package com.lite.task.core.scheduler;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Delay Task Scheduler
 *
 * Polls the delay queue and moves ready tasks to the execution queue.
 * Uses Redis ZSet (score = execution timestamp) for delay queue.
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DelayTaskScheduler {

    private final DelayQueueOperator delayQueueOperator;
    private final TaskQueueOperator taskQueueOperator;
    private final TaskCacheOperator taskCacheOperator;

    @Value("${task.scheduler.delay.batch-size:100}")
    private int batchSize;

    @Value("${task.scheduler.delay.enabled:true}")
    private boolean enabled;

    /**
     * Poll delay queue and move ready tasks to execution queue.
     * Runs every second by default.
     */
    @Scheduled(fixedDelayString = "${task.scheduler.delay.interval:1000}")
    public void pollAndTransfer() {
        if (!enabled) {
            return;
        }

        try {
            // Poll ready tasks from delay queue
            List<String> readyTaskIds = delayQueueOperator.pollReady(batchSize);

            if (readyTaskIds.isEmpty()) {
                return;
            }

            int transferred = 0;
            for (String taskId : readyTaskIds) {
                if (transferToExecutionQueue(taskId)) {
                    transferred++;
                }
            }

            if (transferred > 0) {
                log.info("Transferred {} delayed tasks to execution queue", transferred);
            }

        } catch (Exception e) {
            log.error("Error polling delay queue: {}", e.getMessage(), e);
        }
    }

    /**
     * Transfer a single task from delay queue to execution queue.
     *
     * @param taskId Task ID
     * @return true if transferred successfully
     */
    private boolean transferToExecutionQueue(String taskId) {
        try {
            // Get task from cache to determine priority
            TaskInstance task = taskCacheOperator.get(taskId);

            if (task == null) {
                log.warn("Task not found in cache, skipping: {}", taskId);
                return false;
            }

            // Get priority
            TaskPriority priority = task.getPriorityEnum();
            if (priority == null) {
                priority = TaskPriority.DEFAULT;
            }

            // Push to execution queue
            taskQueueOperator.push(taskId, priority);

            log.debug("Transferred delayed task to queue: taskId={}, priority={}",
                    taskId, priority);
            return true;

        } catch (Exception e) {
            log.error("Failed to transfer task {}: {}", taskId, e.getMessage());
            return false;
        }
    }

    /**
     * Get current delay queue size.
     */
    public long getDelayQueueSize() {
        return delayQueueOperator.size();
    }

    /**
     * Get count of tasks ready to execute.
     */
    public long getReadyCount() {
        return delayQueueOperator.readyCount();
    }
}
