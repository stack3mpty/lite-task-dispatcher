package com.lite.task.common.exception;

import lombok.Getter;

/**
 * Rate Limit Exception
 *
 * Thrown when rate limit is exceeded
 *
 * @author lite-task-dispatcher
 */
@Getter
public class RateLimitException extends TaskException {

    private final String taskType;
    private final int limit;
    private final long retryAfterMs;

    public RateLimitException(String taskType, int limit) {
        super(ErrorCode.RATE_LIMIT_EXCEEDED,
                String.format("Rate limit exceeded for task type: %s, limit: %d/s", taskType, limit));
        this.taskType = taskType;
        this.limit = limit;
        this.retryAfterMs = 1000; // Default retry after 1 second
    }

    public RateLimitException(String taskType, int limit, long retryAfterMs) {
        super(ErrorCode.RATE_LIMIT_EXCEEDED,
                String.format("Rate limit exceeded for task type: %s, limit: %d/s, retry after: %dms",
                        taskType, limit, retryAfterMs));
        this.taskType = taskType;
        this.limit = limit;
        this.retryAfterMs = retryAfterMs;
    }

    @Override
    public boolean isRetryable() {
        return true; // Rate limit errors are always retryable after some delay
    }
}
