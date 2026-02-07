package com.lite.task.infrastructure.redis;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.util.JsonUtils;
import com.lite.task.domain.task.entity.TaskInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Task Cache Operator
 *
 * Redis Hash based task data storage (primary storage)
 * Key format: task:data:{taskId}
 *
 * Note: result field is NOT stored in Redis, it's written directly to DB by executor
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskCacheOperator {

    private final StringRedisTemplate redisTemplate;

    private static final String TASK_DATA_KEY_PREFIX = "task:data:";
    private static final long TERMINAL_TASK_TTL_HOURS = 24;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    // ==================== Core CRUD Methods ====================

    /**
     * Save task to Redis Hash
     *
     * @param task TaskInstance to save
     */
    public void save(TaskInstance task) {
        String key = getTaskDataKey(task.getTaskId());
        Map<String, String> hash = taskToHash(task);

        redisTemplate.opsForHash().putAll(key, hash);

        // Set TTL for terminal tasks
        if (task.getStatus().isTerminal()) {
            redisTemplate.expire(key, TERMINAL_TASK_TTL_HOURS, TimeUnit.HOURS);
        }

        log.debug("Saved task to cache: taskId={}, status={}", task.getTaskId(), task.getStatus());
    }

    /**
     * Get task from Redis Hash
     *
     * @param taskId Task ID
     * @return TaskInstance or null if not found
     */
    public TaskInstance get(String taskId) {
        String key = getTaskDataKey(taskId);
        Map<Object, Object> hash = redisTemplate.opsForHash().entries(key);

        if (hash == null || hash.isEmpty()) {
            return null;
        }

        return hashToTask(hash);
    }

    /**
     * Check if task exists in cache
     *
     * @param taskId Task ID
     * @return true if exists
     */
    public boolean exists(String taskId) {
        String key = getTaskDataKey(taskId);
        return Boolean.TRUE.equals(redisTemplate.hasKey(key));
    }

    /**
     * Delete task from cache
     *
     * @param taskId Task ID
     * @return true if deleted
     */
    public boolean delete(String taskId) {
        String key = getTaskDataKey(taskId);
        Boolean result = redisTemplate.delete(key);
        if (Boolean.TRUE.equals(result)) {
            log.debug("Deleted task from cache: taskId={}", taskId);
        }
        return Boolean.TRUE.equals(result);
    }

    // ==================== Partial Update Methods ====================

    /**
     * Update task status
     */
    public void updateStatus(String taskId, TaskStatus status) {
        String key = getTaskDataKey(taskId);
        Map<String, String> updates = new HashMap<>();
        updates.put("status", status.getCode());
        updates.put("updatedAt", formatDateTime(LocalDateTime.now()));

        redisTemplate.opsForHash().putAll(key, updates);

        // Set TTL if terminal
        if (status.isTerminal()) {
            redisTemplate.expire(key, TERMINAL_TASK_TTL_HOURS, TimeUnit.HOURS);
        }

        log.debug("Updated task status: taskId={}, status={}", taskId, status);
    }

    /**
     * Update task on start execution
     */
    public void updateOnStart(String taskId, String executorId) {
        String key = getTaskDataKey(taskId);
        LocalDateTime now = LocalDateTime.now();

        Map<String, String> updates = new HashMap<>();
        updates.put("status", TaskStatus.RUNNING.getCode());
        updates.put("executorId", executorId != null ? executorId : "");
        updates.put("startedAt", formatDateTime(now));
        updates.put("updatedAt", formatDateTime(now));

        redisTemplate.opsForHash().putAll(key, updates);
        log.debug("Updated task on start: taskId={}, executorId={}", taskId, executorId);
    }

    /**
     * Update task on completion (status only, result is stored in DB by executor)
     */
    public void updateOnComplete(String taskId) {
        String key = getTaskDataKey(taskId);
        LocalDateTime now = LocalDateTime.now();

        Map<String, String> updates = new HashMap<>();
        updates.put("status", TaskStatus.SUCCESS.getCode());
        updates.put("finishedAt", formatDateTime(now));
        updates.put("updatedAt", formatDateTime(now));

        redisTemplate.opsForHash().putAll(key, updates);
        redisTemplate.expire(key, TERMINAL_TASK_TTL_HOURS, TimeUnit.HOURS);

        log.debug("Updated task on complete: taskId={}", taskId);
    }

    /**
     * Update task on failure
     */
    public void updateOnFailure(String taskId, String errorMessage, TaskStatus newStatus) {
        String key = getTaskDataKey(taskId);
        LocalDateTime now = LocalDateTime.now();

        Map<String, String> updates = new HashMap<>();
        updates.put("status", newStatus.getCode());
        updates.put("errorMessage", errorMessage != null ? errorMessage : "");
        updates.put("finishedAt", formatDateTime(now));
        updates.put("updatedAt", formatDateTime(now));

        redisTemplate.opsForHash().putAll(key, updates);

        if (newStatus.isTerminal()) {
            redisTemplate.expire(key, TERMINAL_TASK_TTL_HOURS, TimeUnit.HOURS);
        }

        log.debug("Updated task on failure: taskId={}, status={}", taskId, newStatus);
    }

    /**
     * Update task for retry
     */
    public void updateRetry(String taskId, int retryCount, TaskStatus status) {
        String key = getTaskDataKey(taskId);
        LocalDateTime now = LocalDateTime.now();

        Map<String, String> updates = new HashMap<>();
        updates.put("status", status.getCode());
        updates.put("retryCount", String.valueOf(retryCount));
        updates.put("startedAt", "");
        updates.put("finishedAt", "");
        updates.put("executorId", "");
        updates.put("errorMessage", "");
        updates.put("updatedAt", formatDateTime(now));

        redisTemplate.opsForHash().putAll(key, updates);
        log.debug("Updated task for retry: taskId={}, retryCount={}", taskId, retryCount);
    }

    // ==================== Field Access Methods ====================

    /**
     * Get single field value
     */
    public String getField(String taskId, String field) {
        String key = getTaskDataKey(taskId);
        Object value = redisTemplate.opsForHash().get(key, field);
        return value != null ? value.toString() : null;
    }

    /**
     * Get task status
     */
    public TaskStatus getStatus(String taskId) {
        String statusStr = getField(taskId, "status");
        if (statusStr == null || statusStr.isEmpty()) {
            return null;
        }
        try {
            return TaskStatus.fromCode(statusStr);
        } catch (IllegalArgumentException e) {
            log.warn("Invalid status code in cache: taskId={}, status={}", taskId, statusStr);
            return null;
        }
    }

    /**
     * Get task type
     */
    public String getTaskType(String taskId) {
        return getField(taskId, "taskType");
    }

    /**
     * Get retry count
     */
    public int getRetryCount(String taskId) {
        String retryCountStr = getField(taskId, "retryCount");
        return retryCountStr != null && !retryCountStr.isEmpty() ? Integer.parseInt(retryCountStr) : 0;
    }

    // ==================== Helper Methods ====================

    private String getTaskDataKey(String taskId) {
        return TASK_DATA_KEY_PREFIX + taskId;
    }

    /**
     * Convert TaskInstance to Redis Hash map
     * Note: result field is NOT included
     */
    private Map<String, String> taskToHash(TaskInstance task) {
        Map<String, String> hash = new HashMap<>();

        hash.put("id", task.getId() != null ? String.valueOf(task.getId()) : "");
        hash.put("taskId", task.getTaskId());
        hash.put("taskDefId", task.getTaskDefId() != null ? String.valueOf(task.getTaskDefId()) : "");
        hash.put("taskType", task.getTaskType() != null ? task.getTaskType() : "");
        hash.put("status", task.getStatus() != null ? task.getStatus().getCode() : TaskStatus.CREATED.getCode());
        hash.put("priority", String.valueOf(task.getPriority() != null ? task.getPriority() : TaskPriority.DEFAULT.getLevel()));
        hash.put("params", task.getParams() != null ? JsonUtils.toJson(task.getParams()) : "");
        // result is NOT stored in Redis - it's written directly to DB by executor
        hash.put("errorMessage", task.getErrorMessage() != null ? task.getErrorMessage() : "");
        hash.put("retryCount", String.valueOf(task.getRetryCount() != null ? task.getRetryCount() : 0));
        hash.put("maxRetry", String.valueOf(task.getMaxRetry() != null ? task.getMaxRetry() : 3));
        hash.put("executeAt", formatDateTime(task.getExecuteAt()));
        hash.put("startedAt", formatDateTime(task.getStartedAt()));
        hash.put("finishedAt", formatDateTime(task.getFinishedAt()));
        hash.put("callbackUrl", task.getCallbackUrl() != null ? task.getCallbackUrl() : "");
        hash.put("createdBy", task.getCreatedBy() != null ? task.getCreatedBy() : "");
        hash.put("executorId", task.getExecutorId() != null ? task.getExecutorId() : "");
        hash.put("createdAt", formatDateTime(task.getCreatedAt()));
        hash.put("updatedAt", formatDateTime(task.getUpdatedAt()));

        return hash;
    }

    /**
     * Convert Redis Hash map to TaskInstance
     * Note: result field will be null (not stored in Redis)
     */
    private TaskInstance hashToTask(Map<Object, Object> hash) {
        return TaskInstance.builder()
                .id(getLong(hash, "id"))
                .taskId(getString(hash, "taskId"))
                .taskDefId(getLong(hash, "taskDefId"))
                .taskType(getString(hash, "taskType"))
                .status(getStatus(hash, "status"))
                .priority(getInt(hash, "priority", TaskPriority.DEFAULT.getLevel()))
                .params(getMap(hash, "params"))
                .result(null) // result is NOT stored in Redis
                .errorMessage(getString(hash, "errorMessage"))
                .retryCount(getInt(hash, "retryCount", 0))
                .maxRetry(getInt(hash, "maxRetry", 3))
                .executeAt(getDateTime(hash, "executeAt"))
                .startedAt(getDateTime(hash, "startedAt"))
                .finishedAt(getDateTime(hash, "finishedAt"))
                .callbackUrl(getString(hash, "callbackUrl"))
                .createdBy(getString(hash, "createdBy"))
                .executorId(getString(hash, "executorId"))
                .createdAt(getDateTime(hash, "createdAt"))
                .updatedAt(getDateTime(hash, "updatedAt"))
                .build();
    }

    private String getString(Map<Object, Object> hash, String key) {
        Object value = hash.get(key);
        if (value == null) {
            return null;
        }
        String str = value.toString();
        return str.isEmpty() ? null : str;
    }

    private int getInt(Map<Object, Object> hash, String key, int defaultValue) {
        Object value = hash.get(key);
        if (value == null || value.toString().isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private Long getLong(Map<Object, Object> hash, String key) {
        Object value = hash.get(key);
        if (value == null || value.toString().isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private TaskStatus getStatus(Map<Object, Object> hash, String key) {
        Object value = hash.get(key);
        if (value == null || value.toString().isEmpty()) {
            return TaskStatus.CREATED;
        }
        try {
            return TaskStatus.fromCode(value.toString());
        } catch (IllegalArgumentException e) {
            return TaskStatus.CREATED;
        }
    }

    private LocalDateTime getDateTime(Map<Object, Object> hash, String key) {
        Object value = hash.get(key);
        if (value == null || value.toString().isEmpty()) {
            return null;
        }
        try {
            return LocalDateTime.parse(value.toString(), DATE_TIME_FORMATTER);
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getMap(Map<Object, Object> hash, String key) {
        Object value = hash.get(key);
        if (value == null || value.toString().isEmpty()) {
            return null;
        }
        try {
            return JsonUtils.fromJsonMap(value.toString());
        } catch (Exception e) {
            return null;
        }
    }

    private String formatDateTime(LocalDateTime dateTime) {
        return dateTime != null ? dateTime.format(DATE_TIME_FORMATTER) : "";
    }
}
