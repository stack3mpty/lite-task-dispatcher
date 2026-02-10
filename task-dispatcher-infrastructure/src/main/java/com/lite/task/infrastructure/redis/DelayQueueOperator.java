package com.lite.task.infrastructure.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Delay Queue Operator
 *
 * Redis-based delay queue using ZSet structure
 * Score = execution timestamp (milliseconds)
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DelayQueueOperator {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<List> pollReadyScript = buildPollReadyScript();

    private static final String DELAY_QUEUE_KEY = "task:delay:queue";
    private static final String POLL_READY_LUA_SCRIPT = """
            local key = KEYS[1]
            local now = tonumber(ARGV[1])
            local limit = tonumber(ARGV[2])

            local tasks = redis.call('ZRANGEBYSCORE', key, '-inf', now, 'LIMIT', 0, limit)
            if #tasks > 0 then
                redis.call('ZREM', key, unpack(tasks))
            end
            return tasks
            """;

    /**
     * Add task to delay queue
     *
     * @param taskId    Task ID
     * @param executeAt Scheduled execution time
     */
    public void add(String taskId, LocalDateTime executeAt) {
        long score = executeAt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        redisTemplate.opsForZSet().add(DELAY_QUEUE_KEY, taskId, score);
        log.debug("Added task {} to delay queue, execute at: {}", taskId, executeAt);
    }

    /**
     * Add task to delay queue with delay in milliseconds
     *
     * @param taskId  Task ID
     * @param delayMs Delay in milliseconds
     */
    public void addWithDelay(String taskId, long delayMs) {
        long score = System.currentTimeMillis() + delayMs;
        redisTemplate.opsForZSet().add(DELAY_QUEUE_KEY, taskId, score);
        log.debug("Added task {} to delay queue, delay: {}ms", taskId, delayMs);
    }

    /**
     * Poll ready tasks (tasks whose execution time has passed)
     *
     * @param limit Maximum number of tasks to poll
     * @return List of ready task IDs
     */
    public List<String> pollReady(int limit) {
        long now = System.currentTimeMillis();
        if (limit <= 0) {
            return Collections.emptyList();
        }

        @SuppressWarnings("unchecked")
        List<String> taskIds = (List<String>) redisTemplate.execute(
                pollReadyScript,
                Collections.singletonList(DELAY_QUEUE_KEY),
                String.valueOf(now),
                String.valueOf(limit)
        );
        if (taskIds == null || taskIds.isEmpty()) {
            return Collections.emptyList();
        }

        log.debug("Polled {} ready tasks from delay queue", taskIds.size());
        return taskIds;
    }

    /**
     * Peek at next ready task without removing
     *
     * @return Task ID or null if no ready tasks
     */
    public String peekReady() {
        long now = System.currentTimeMillis();
        Set<String> tasks = redisTemplate.opsForZSet().rangeByScore(DELAY_QUEUE_KEY, 0, now, 0, 1);
        return (tasks != null && !tasks.isEmpty()) ? tasks.iterator().next() : null;
    }

    /**
     * Get time until next task is ready
     *
     * @return Milliseconds until next task is ready, or -1 if queue is empty
     */
    public long getTimeUntilNextReady() {
        Set<ZSetOperations.TypedTuple<String>> tasks = redisTemplate.opsForZSet()
                .rangeWithScores(DELAY_QUEUE_KEY, 0, 0);

        if (tasks == null || tasks.isEmpty()) {
            return -1;
        }

        Double score = tasks.iterator().next().getScore();
        if (score == null) {
            return -1;
        }

        long nextTime = score.longValue();
        long now = System.currentTimeMillis();
        return Math.max(0, nextTime - now);
    }

    /**
     * Remove task from delay queue
     *
     * @param taskId Task ID
     * @return true if removed
     */
    public boolean remove(String taskId) {
        Long removed = redisTemplate.opsForZSet().remove(DELAY_QUEUE_KEY, taskId);
        return removed != null && removed > 0;
    }

    /**
     * Get queue size
     */
    public long size() {
        Long size = redisTemplate.opsForZSet().size(DELAY_QUEUE_KEY);
        return size != null ? size : 0;
    }

    /**
     * Get count of ready tasks
     */
    public long readyCount() {
        long now = System.currentTimeMillis();
        Long count = redisTemplate.opsForZSet().count(DELAY_QUEUE_KEY, 0, now);
        return count != null ? count : 0;
    }

    /**
     * Get scheduled execution time for a task
     *
     * @param taskId Task ID
     * @return Execution time or null if not found
     */
    public LocalDateTime getExecuteTime(String taskId) {
        Double score = redisTemplate.opsForZSet().score(DELAY_QUEUE_KEY, taskId);
        if (score == null) {
            return null;
        }
        return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(score.longValue()),
                ZoneId.systemDefault()
        );
    }

    /**
     * Update execution time for a task
     *
     * @param taskId    Task ID
     * @param executeAt New execution time
     * @return true if updated
     */
    public boolean updateExecuteTime(String taskId, LocalDateTime executeAt) {
        Double currentScore = redisTemplate.opsForZSet().score(DELAY_QUEUE_KEY, taskId);
        if (currentScore == null) {
            return false;
        }

        long newScore = executeAt.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        redisTemplate.opsForZSet().add(DELAY_QUEUE_KEY, taskId, newScore);
        return true;
    }

    /**
     * Check if task exists in delay queue
     */
    public boolean exists(String taskId) {
        Double score = redisTemplate.opsForZSet().score(DELAY_QUEUE_KEY, taskId);
        return score != null;
    }

    private DefaultRedisScript<List> buildPollReadyScript() {
        DefaultRedisScript<List> script = new DefaultRedisScript<>();
        script.setScriptText(POLL_READY_LUA_SCRIPT);
        script.setResultType(List.class);
        return script;
    }
}
