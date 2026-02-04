package com.lite.task.core.executor.retry;

/**
 * Retry Policy Interface
 *
 * Defines retry behavior for failed tasks
 *
 * @author lite-task-dispatcher
 */
public interface RetryPolicy {

    /**
     * Check if task should be retried
     *
     * @param currentAttempt  Current attempt number (1-based)
     * @param lastException   Last exception that caused failure
     * @return true if should retry
     */
    boolean shouldRetry(int currentAttempt, Exception lastException);

    /**
     * Calculate next retry delay in milliseconds
     *
     * @param currentAttempt Current attempt number (1-based)
     * @return Delay in milliseconds before next retry
     */
    long getNextDelay(int currentAttempt);

    /**
     * Get maximum number of attempts
     *
     * @return Max attempts
     */
    int getMaxAttempts();
}
