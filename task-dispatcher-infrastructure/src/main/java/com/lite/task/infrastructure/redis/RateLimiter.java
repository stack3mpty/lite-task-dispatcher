package com.lite.task.infrastructure.redis;

import com.lite.task.common.exception.RateLimitException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Rate Limiter
 *
 * Redis + Lua based token bucket rate limiter
 *
 * Features:
 * - Atomic operations using Lua script
 * - Token bucket algorithm
 * - Per-task-type rate limiting
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
public class RateLimiter {

    private final StringRedisTemplate redisTemplate;
    private final DefaultRedisScript<Long> rateLimitScript;

    private static final String RATE_LIMIT_KEY_PREFIX = "task:rate:limit:";

    /**
     * Lua script for token bucket rate limiting
     *
     * KEYS[1] = rate limit key
     * ARGV[1] = bucket capacity
     * ARGV[2] = refill rate (tokens per second)
     * ARGV[3] = current timestamp (milliseconds)
     * ARGV[4] = requested tokens
     *
     * Returns: 1 if allowed, 0 if rejected
     */
    private static final String RATE_LIMIT_LUA_SCRIPT = """
            local key = KEYS[1]
            local capacity = tonumber(ARGV[1])
            local rate = tonumber(ARGV[2])
            local now = tonumber(ARGV[3])
            local requested = tonumber(ARGV[4])

            local data = redis.call('HMGET', key, 'tokens', 'timestamp')
            local tokens = tonumber(data[1]) or capacity
            local last_time = tonumber(data[2]) or now

            -- Calculate tokens to add based on time elapsed
            local delta = math.max(0, now - last_time)
            local filled = math.min(capacity, tokens + (delta * rate / 1000))

            -- Check if we have enough tokens
            if filled >= requested then
                local new_tokens = filled - requested
                redis.call('HMSET', key, 'tokens', new_tokens, 'timestamp', now)
                redis.call('EXPIRE', key, 60)
                return 1
            else
                -- Update timestamp even if rejected to maintain accurate token count
                redis.call('HMSET', key, 'tokens', filled, 'timestamp', now)
                redis.call('EXPIRE', key, 60)
                return 0
            end
            """;

    public RateLimiter(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
        this.rateLimitScript = new DefaultRedisScript<>();
        this.rateLimitScript.setScriptText(RATE_LIMIT_LUA_SCRIPT);
        this.rateLimitScript.setResultType(Long.class);
    }

    /**
     * Try to acquire permit
     *
     * @param taskType Task type (rate limit key)
     * @param capacity Bucket capacity
     * @param rate     Refill rate (tokens per second)
     * @return true if permit acquired
     */
    public boolean tryAcquire(String taskType, int capacity, int rate) {
        return tryAcquire(taskType, capacity, rate, 1);
    }

    /**
     * Try to acquire multiple permits
     *
     * @param taskType Task type (rate limit key)
     * @param capacity Bucket capacity
     * @param rate     Refill rate (tokens per second)
     * @param permits  Number of permits to acquire
     * @return true if permits acquired
     */
    public boolean tryAcquire(String taskType, int capacity, int rate, int permits) {
        String key = getRateLimitKey(taskType);
        long now = System.currentTimeMillis();

        List<String> keys = Collections.singletonList(key);
        Long result = redisTemplate.execute(
                rateLimitScript,
                keys,
                String.valueOf(capacity),
                String.valueOf(rate),
                String.valueOf(now),
                String.valueOf(permits)
        );

        boolean acquired = result != null && result == 1;
        if (!acquired) {
            log.debug("Rate limit exceeded for task type: {}, capacity: {}, rate: {}/s",
                    taskType, capacity, rate);
        }
        return acquired;
    }

    /**
     * Acquire permit or throw exception
     *
     * @param taskType Task type
     * @param capacity Bucket capacity
     * @param rate     Refill rate
     * @throws RateLimitException if rate limit exceeded
     */
    public void acquire(String taskType, int capacity, int rate) {
        if (!tryAcquire(taskType, capacity, rate)) {
            throw new RateLimitException(taskType, rate);
        }
    }

    /**
     * Get current token count for a task type
     *
     * @param taskType Task type
     * @return Current tokens or -1 if not exists
     */
    public long getCurrentTokens(String taskType) {
        String key = getRateLimitKey(taskType);
        Object tokens = redisTemplate.opsForHash().get(key, "tokens");
        if (tokens == null) {
            return -1;
        }
        return Long.parseLong(tokens.toString());
    }

    /**
     * Reset rate limit for a task type
     *
     * @param taskType Task type
     */
    public void reset(String taskType) {
        String key = getRateLimitKey(taskType);
        redisTemplate.delete(key);
        log.info("Rate limit reset for task type: {}", taskType);
    }

    /**
     * Check if rate limit exists for task type
     */
    public boolean exists(String taskType) {
        String key = getRateLimitKey(taskType);
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    private String getRateLimitKey(String taskType) {
        return RATE_LIMIT_KEY_PREFIX + taskType;
    }
}
