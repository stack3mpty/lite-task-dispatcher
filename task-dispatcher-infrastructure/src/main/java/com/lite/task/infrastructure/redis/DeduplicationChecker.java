package com.lite.task.infrastructure.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Deduplication Checker
 *
 * Redis-based task deduplication using Set/String
 *
 * Features:
 * - MD5 hash for efficient storage
 * - TTL-based automatic expiration
 * - Support for parameter-based deduplication
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeduplicationChecker {

    private final StringRedisTemplate redisTemplate;

    private static final String DEDUP_KEY_PREFIX = "task:dedup:";
    private static final Duration DEFAULT_TTL = Duration.ofHours(24);

    /**
     * Check if task is duplicate
     *
     * @param taskType Task type
     * @param params   Task parameters
     * @return true if duplicate
     */
    public boolean isDuplicate(String taskType, Map<String, Object> params) {
        String hash = generateHash(taskType, params);
        String key = getDeduplicationKey(taskType, hash);
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    /**
     * Check if task is duplicate by custom key
     */
    public boolean isDuplicate(String taskType, String uniqueKey) {
        String hash = md5(uniqueKey);
        String key = getDeduplicationKey(taskType, hash);
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    /**
     * Mark task as processed (add to deduplication set)
     *
     * @param taskType Task type
     * @param params   Task parameters
     * @param ttl      Time to live
     */
    public void markAsProcessed(String taskType, Map<String, Object> params, Duration ttl) {
        String hash = generateHash(taskType, params);
        String key = getDeduplicationKey(taskType, hash);
        redisTemplate.opsForValue().set(key, "1", ttl);
        log.debug("Marked task as processed: {}, hash: {}", taskType, hash);
    }

    /**
     * Mark task as processed with default TTL
     */
    public void markAsProcessed(String taskType, Map<String, Object> params) {
        markAsProcessed(taskType, params, DEFAULT_TTL);
    }

    /**
     * Mark task as processed by custom key
     */
    public void markAsProcessed(String taskType, String uniqueKey, Duration ttl) {
        String hash = md5(uniqueKey);
        String key = getDeduplicationKey(taskType, hash);
        redisTemplate.opsForValue().set(key, "1", ttl);
        log.debug("Marked task as processed: {}, key: {}", taskType, uniqueKey);
    }

    /**
     * Check and mark - returns true if NOT duplicate and marks as processed
     *
     * @param taskType Task type
     * @param params   Task parameters
     * @param ttl      Time to live
     * @return true if task is NOT duplicate (first occurrence)
     */
    public boolean checkAndMark(String taskType, Map<String, Object> params, Duration ttl) {
        String hash = generateHash(taskType, params);
        String key = getDeduplicationKey(taskType, hash);

        // Use setIfAbsent for atomic check-and-set
        Boolean result = redisTemplate.opsForValue().setIfAbsent(key, "1", ttl);
        boolean isNew = Boolean.TRUE.equals(result);

        if (!isNew) {
            log.debug("Duplicate task detected: {}, hash: {}", taskType, hash);
        }

        return isNew;
    }

    /**
     * Check and mark with default TTL
     */
    public boolean checkAndMark(String taskType, Map<String, Object> params) {
        return checkAndMark(taskType, params, DEFAULT_TTL);
    }

    /**
     * Check and mark by task ID
     */
    public boolean checkAndMarkByTaskId(String taskType, String taskId, Duration ttl) {
        String key = getDeduplicationKey(taskType, taskId);
        Boolean result = redisTemplate.opsForValue().setIfAbsent(key, "1", ttl);
        return Boolean.TRUE.equals(result);
    }

    /**
     * Remove deduplication mark
     */
    public void remove(String taskType, Map<String, Object> params) {
        String hash = generateHash(taskType, params);
        String key = getDeduplicationKey(taskType, hash);
        redisTemplate.delete(key);
        log.debug("Removed deduplication mark: {}, hash: {}", taskType, hash);
    }

    /**
     * Remove by task ID
     */
    public void removeByTaskId(String taskType, String taskId) {
        String key = getDeduplicationKey(taskType, taskId);
        redisTemplate.delete(key);
    }

    /**
     * Get remaining TTL for deduplication key
     *
     * @return TTL in seconds, -1 if no expiry, -2 if key doesn't exist
     */
    public long getTtl(String taskType, Map<String, Object> params) {
        String hash = generateHash(taskType, params);
        String key = getDeduplicationKey(taskType, hash);
        Long ttl = redisTemplate.getExpire(key, TimeUnit.SECONDS);
        return ttl != null ? ttl : -2;
    }

    /**
     * Generate hash from task type and parameters
     */
    private String generateHash(String taskType, Map<String, Object> params) {
        StringBuilder sb = new StringBuilder();
        sb.append(taskType).append(":");

        if (params != null && !params.isEmpty()) {
            // Sort keys for consistent hashing
            params.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        sb.append(entry.getKey()).append("=");
                        sb.append(entry.getValue()).append("&");
                    });
        }

        return md5(sb.toString());
    }

    /**
     * Calculate MD5 hash
     */
    private String md5(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("MD5 algorithm not found", e);
        }
    }

    private String getDeduplicationKey(String taskType, String hash) {
        return DEDUP_KEY_PREFIX + taskType + ":" + hash;
    }
}
