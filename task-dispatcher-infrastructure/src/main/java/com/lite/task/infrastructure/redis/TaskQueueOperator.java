package com.lite.task.infrastructure.redis;

import com.lite.task.common.enums.TaskPriority;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    private static final String PROCESSING_KEY_PREFIX = "task:queue:processing:";
    private static final String RESERVATION_KEY_PREFIX = "task:queue:reservation:";
    private static final String RESERVATION_TIMEOUT_INDEX_KEY = "task:queue:reservation:timeout";
    private static final String RUNNING_KEY_PREFIX = "task:running:";
    private final DefaultRedisScript<Long> requeueScript = buildRequeueScript();

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
     * Reserve from highest priority queue that has tasks.
     * Reserve operation atomically moves task from ready queue -> processing queue.
     */
    public ReservedTask reserveByPriority(String executorId, int visibilityTimeoutSeconds) {
        for (TaskPriority priority : TaskPriority.values()) {
            ReservedTask reservedTask = reserve(priority, executorId, visibilityTimeoutSeconds);
            if (reservedTask != null) {
                return reservedTask;
            }
        }
        return null;
    }

    /**
     * Reserve one task from specific priority queue.
     */
    public ReservedTask reserve(TaskPriority priority, String executorId, int visibilityTimeoutSeconds) {
        String queueKey = getQueueKey(priority);
        String processingKey = getProcessingKey(priority);
        String taskId = redisTemplate.opsForList().rightPopAndLeftPush(queueKey, processingKey);
        if (taskId == null) {
            return null;
        }

        long now = System.currentTimeMillis();
        int safeVisibilityTimeout = Math.max(visibilityTimeoutSeconds, 30);
        long deadlineAt = now + (safeVisibilityTimeout * 1000L);

        String reservationKey = getReservationKey(taskId);
        Map<String, String> metadata = new HashMap<>();
        metadata.put("taskId", taskId);
        metadata.put("queueKey", queueKey);
        metadata.put("processingKey", processingKey);
        metadata.put("executorId", executorId != null ? executorId : "");
        metadata.put("reservedAt", String.valueOf(now));
        metadata.put("deadlineAt", String.valueOf(deadlineAt));

        redisTemplate.opsForHash().putAll(reservationKey, metadata);
        redisTemplate.expire(reservationKey, safeVisibilityTimeout + 120L, TimeUnit.SECONDS);
        redisTemplate.opsForZSet().add(RESERVATION_TIMEOUT_INDEX_KEY, taskId, deadlineAt);

        log.debug("Reserved task: taskId={}, priority={}, executorId={}, deadlineAt={}",
                taskId, priority, executorId, deadlineAt);
        return new ReservedTask(taskId, priority);
    }

    /**
     * Ack reserved task and remove from processing queue.
     */
    public void ackReservation(String taskId) {
        ReservationInfo reservationInfo = getReservationInfo(taskId);
        if (reservationInfo != null && reservationInfo.processingKey != null) {
            redisTemplate.opsForList().remove(reservationInfo.processingKey, 1, taskId);
        } else {
            // Best effort cleanup when metadata is missing.
            for (TaskPriority priority : TaskPriority.values()) {
                redisTemplate.opsForList().remove(getProcessingKey(priority), 1, taskId);
            }
        }
        clearReservationMetadata(taskId);
    }

    /**
     * Requeue reserved task back to ready queue.
     *
     * @return true if task was moved from processing queue back to ready queue.
     */
    public boolean requeueReservation(String taskId) {
        ReservationInfo reservationInfo = getReservationInfo(taskId);
        if (reservationInfo == null || reservationInfo.processingKey == null || reservationInfo.queueKey == null) {
            clearReservationMetadata(taskId);
            return false;
        }

        Long moved = redisTemplate.execute(
                requeueScript,
                List.of(
                        reservationInfo.processingKey,
                        reservationInfo.queueKey,
                        RESERVATION_TIMEOUT_INDEX_KEY,
                        getReservationKey(taskId)
                ),
                taskId
        );
        boolean requeued = moved != null && moved > 0;
        if (requeued) {
            log.info("Requeued reserved task to ready queue: taskId={}, queueKey={}",
                    taskId, reservationInfo.queueKey);
        }
        return requeued;
    }

    /**
     * Reclaim expired reservations by moving timed-out processing tasks back to ready queue.
     *
     * @return number of reclaimed tasks.
     */
    public long reclaimExpiredReservations(int limit) {
        int safeLimit = Math.max(limit, 1);
        long now = System.currentTimeMillis();
        Set<String> expiredTaskIds = redisTemplate.opsForZSet()
                .rangeByScore(RESERVATION_TIMEOUT_INDEX_KEY, 0, now, 0, safeLimit);
        if (expiredTaskIds == null || expiredTaskIds.isEmpty()) {
            return 0L;
        }

        long reclaimed = 0L;
        for (String taskId : expiredTaskIds) {
            if (requeueReservation(taskId)) {
                reclaimed++;
            }
        }
        return reclaimed;
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
     * Get processing queue size.
     */
    public long processingSize(TaskPriority priority) {
        Long size = redisTemplate.opsForList().size(getProcessingKey(priority));
        return size != null ? size : 0L;
    }

    /**
     * Get total processing queue size across all priorities.
     */
    public long totalProcessingSize() {
        long total = 0L;
        for (TaskPriority priority : TaskPriority.values()) {
            total += processingSize(priority);
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

    private String getProcessingKey(TaskPriority priority) {
        return PROCESSING_KEY_PREFIX + priority.getQueueSuffix();
    }

    private String getReservationKey(String taskId) {
        return RESERVATION_KEY_PREFIX + taskId;
    }

    private String getRunningKey(String taskId) {
        return RUNNING_KEY_PREFIX + taskId;
    }

    private ReservationInfo getReservationInfo(String taskId) {
        Map<Object, Object> metadata = redisTemplate.opsForHash().entries(getReservationKey(taskId));
        if (metadata == null || metadata.isEmpty()) {
            return null;
        }
        return new ReservationInfo(
                getString(metadata, "queueKey"),
                getString(metadata, "processingKey")
        );
    }

    private void clearReservationMetadata(String taskId) {
        redisTemplate.delete(getReservationKey(taskId));
        redisTemplate.opsForZSet().remove(RESERVATION_TIMEOUT_INDEX_KEY, taskId);
    }

    private String getString(Map<Object, Object> map, String key) {
        Object value = map.get(key);
        if (value == null) {
            return null;
        }
        String text = value.toString();
        return text.isBlank() ? null : text;
    }

    private DefaultRedisScript<Long> buildRequeueScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setResultType(Long.class);
        script.setScriptText("""
                local processing = KEYS[1]
                local queue = KEYS[2]
                local timeoutIndex = KEYS[3]
                local reservationKey = KEYS[4]
                local taskId = ARGV[1]

                local removed = redis.call('LREM', processing, 1, taskId)
                if removed and removed > 0 then
                    redis.call('LPUSH', queue, taskId)
                end
                redis.call('ZREM', timeoutIndex, taskId)
                redis.call('DEL', reservationKey)
                return removed
                """);
        return script;
    }

    public record ReservedTask(String taskId, TaskPriority priority) {
    }

    private record ReservationInfo(String queueKey, String processingKey) {
    }
}
