package com.lite.task.infrastructure.redis;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.util.JsonUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Task Queue Operator
 *
 * Redis-based task queue operations using List structure
 *
 * Key format: task:queue:{priority}
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskQueueOperator {

    private final StringRedisTemplate redisTemplate;

    private static final String QUEUE_KEY_PREFIX = "task:queue:";
    private static final String RUNNING_KEY_PREFIX = "task:running:";

    /**
     * Push task to queue (left push for FIFO)
     *
     * @param taskId   Task ID
     * @param priority Task priority
     */
    public void push(String taskId, TaskPriority priority) {
        String queueKey = getQueueKey(priority);
        redisTemplate.opsForList().leftPush(queueKey, taskId);
        log.debug("Pushed task {} to queue {}", taskId, queueKey);
    }

    /**
     * Push task to queue with specific priority level
     */
    public void push(String taskId, int priorityLevel) {
        push(taskId, TaskPriority.fromLevel(priorityLevel));
    }

    /**
     * Pop task from queue (right pop for FIFO)
     *
     * @param priority Task priority
     * @return Task ID or null if queue is empty
     */
    public String pop(TaskPriority priority) {
        String queueKey = getQueueKey(priority);
        return redisTemplate.opsForList().rightPop(queueKey);
    }

    /**
     * Blocking pop from queue
     *
     * @param priority Task priority
     * @param timeout  Timeout duration
     * @return Task ID or null if timeout
     */
    public String blockingPop(TaskPriority priority, Duration timeout) {
        String queueKey = getQueueKey(priority);
        return redisTemplate.opsForList().rightPop(queueKey, timeout);
    }

    /**
     * Pop from highest priority queue that has tasks
     *
     * @return Task ID or null if all queues are empty
     */
    public String popByPriority() {
        // Check queues from highest to lowest priority
        for (TaskPriority priority : TaskPriority.values()) {
            String taskId = pop(priority);
            if (taskId != null) {
                return taskId;
            }
        }
        return null;
    }

    /**
     * Pop multiple tasks from queue
     */
    public List<String> popBatch(TaskPriority priority, int count) {
        String queueKey = getQueueKey(priority);
        return redisTemplate.opsForList().rightPop(queueKey, count);
    }

    /**
     * Get queue size
     */
    public long size(TaskPriority priority) {
        String queueKey = getQueueKey(priority);
        Long size = redisTemplate.opsForList().size(queueKey);
        return size != null ? size : 0;
    }

    /**
     * Get total queue size across all priorities
     */
    public long totalSize() {
        long total = 0;
        for (TaskPriority priority : TaskPriority.values()) {
            total += size(priority);
        }
        return total;
    }

    /**
     * Remove task from queue (for cancellation)
     */
    public boolean remove(String taskId, TaskPriority priority) {
        String queueKey = getQueueKey(priority);
        Long removed = redisTemplate.opsForList().remove(queueKey, 1, taskId);
        return removed != null && removed > 0;
    }

    /**
     * Peek at front of queue without removing
     */
    public String peek(TaskPriority priority) {
        String queueKey = getQueueKey(priority);
        return redisTemplate.opsForList().index(queueKey, -1);
    }

    /**
     * Mark task as running
     *
     * @param taskId         Task ID
     * @param executorId     Executor ID
     * @param timeoutSeconds Timeout for running status
     */
    public void markRunning(String taskId, String executorId, int timeoutSeconds) {
        String runningKey = getRunningKey(taskId);
        redisTemplate.opsForValue().set(runningKey, executorId, timeoutSeconds, TimeUnit.SECONDS);
    }

    /**
     * Check if task is running
     */
    public boolean isRunning(String taskId) {
        String runningKey = getRunningKey(taskId);
        return Boolean.TRUE.equals(redisTemplate.hasKey(runningKey));
    }

    /**
     * Get executor ID for running task
     */
    public String getRunningExecutor(String taskId) {
        String runningKey = getRunningKey(taskId);
        return redisTemplate.opsForValue().get(runningKey);
    }

    /**
     * Clear running status
     */
    public void clearRunning(String taskId) {
        String runningKey = getRunningKey(taskId);
        redisTemplate.delete(runningKey);
    }

    /**
     * Extend running timeout (heartbeat)
     */
    public boolean extendRunning(String taskId, int timeoutSeconds) {
        String runningKey = getRunningKey(taskId);
        return Boolean.TRUE.equals(redisTemplate.expire(runningKey, timeoutSeconds, TimeUnit.SECONDS));
    }

    private String getQueueKey(TaskPriority priority) {
        return QUEUE_KEY_PREFIX + priority.getQueueSuffix();
    }

    private String getRunningKey(String taskId) {
        return RUNNING_KEY_PREFIX + taskId;
    }
}
