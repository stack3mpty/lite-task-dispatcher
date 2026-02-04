package com.lite.task.core.executor;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Task Execution Result
 *
 * @author lite-task-dispatcher
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskResult implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Execution success flag
     */
    private boolean success;

    /**
     * Result message
     */
    private String message;

    /**
     * Result data
     */
    private Map<String, Object> data;

    /**
     * Error code (if failed)
     */
    private String errorCode;

    /**
     * Error detail (stack trace, etc.)
     */
    private String errorDetail;

    /**
     * Whether the error is retryable
     */
    private boolean retryable;

    /**
     * Execution duration in milliseconds
     */
    private long durationMs;

    /**
     * Create success result
     */
    public static TaskResult success() {
        return TaskResult.builder()
                .success(true)
                .message("Task executed successfully")
                .build();
    }

    /**
     * Create success result with message
     */
    public static TaskResult success(String message) {
        return TaskResult.builder()
                .success(true)
                .message(message)
                .build();
    }

    /**
     * Create success result with data
     */
    public static TaskResult success(Map<String, Object> data) {
        return TaskResult.builder()
                .success(true)
                .message("Task executed successfully")
                .data(data)
                .build();
    }

    /**
     * Create success result with message and data
     */
    public static TaskResult success(String message, Map<String, Object> data) {
        return TaskResult.builder()
                .success(true)
                .message(message)
                .data(data)
                .build();
    }

    /**
     * Create failure result
     */
    public static TaskResult failure(String message) {
        return TaskResult.builder()
                .success(false)
                .message(message)
                .retryable(true)
                .build();
    }

    /**
     * Create failure result with error code
     */
    public static TaskResult failure(String errorCode, String message) {
        return TaskResult.builder()
                .success(false)
                .errorCode(errorCode)
                .message(message)
                .retryable(true)
                .build();
    }

    /**
     * Create failure result with error detail
     */
    public static TaskResult failure(String message, String errorDetail, boolean retryable) {
        return TaskResult.builder()
                .success(false)
                .message(message)
                .errorDetail(errorDetail)
                .retryable(retryable)
                .build();
    }

    /**
     * Create non-retryable failure
     */
    public static TaskResult permanentFailure(String message) {
        return TaskResult.builder()
                .success(false)
                .message(message)
                .retryable(false)
                .build();
    }

    /**
     * Create non-retryable failure with detail
     */
    public static TaskResult permanentFailure(String message, String errorDetail) {
        return TaskResult.builder()
                .success(false)
                .message(message)
                .errorDetail(errorDetail)
                .retryable(false)
                .build();
    }

    /**
     * Set duration and return self
     */
    public TaskResult withDuration(long durationMs) {
        this.durationMs = durationMs;
        return this;
    }

    /**
     * Add data entry
     */
    public TaskResult addData(String key, Object value) {
        if (this.data == null) {
            this.data = new java.util.HashMap<>();
        }
        this.data.put(key, value);
        return this;
    }
}
