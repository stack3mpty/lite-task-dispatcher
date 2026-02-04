package com.lite.task.core.executor.retry;

import lombok.Builder;
import lombok.Getter;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential Backoff Retry Policy
 *
 * Implements exponential backoff with jitter to avoid thundering herd effect
 *
 * Formula: delay = min(initialDelay * (multiplier ^ attempt) + jitter, maxDelay)
 *
 * @author lite-task-dispatcher
 */
@Getter
@Builder
public class ExponentialBackoffRetry implements RetryPolicy {

    /**
     * Maximum number of retry attempts
     */
    @Builder.Default
    private final int maxAttempts = 3;

    /**
     * Initial delay in milliseconds
     */
    @Builder.Default
    private final long initialDelay = 1000L;

    /**
     * Maximum delay in milliseconds
     */
    @Builder.Default
    private final long maxDelay = 60000L;

    /**
     * Backoff multiplier
     */
    @Builder.Default
    private final double multiplier = 2.0;

    /**
     * Jitter factor (0.0 to 1.0)
     * Adds randomness to prevent thundering herd
     */
    @Builder.Default
    private final double jitterFactor = 0.2;

    /**
     * Exception types that should NOT be retried
     */
    @Builder.Default
    private final Set<Class<? extends Exception>> nonRetryableExceptions = new HashSet<>();

    @Override
    public boolean shouldRetry(int currentAttempt, Exception lastException) {
        // Check if max attempts exceeded
        if (currentAttempt >= maxAttempts) {
            return false;
        }

        // Check if exception is non-retryable
        if (lastException != null) {
            for (Class<? extends Exception> nonRetryable : nonRetryableExceptions) {
                if (nonRetryable.isInstance(lastException)) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public long getNextDelay(int currentAttempt) {
        // Calculate base delay with exponential backoff
        // attempt is 1-based, so we use (attempt - 1) for exponent
        double baseDelay = initialDelay * Math.pow(multiplier, currentAttempt - 1);

        // Cap at max delay
        baseDelay = Math.min(baseDelay, maxDelay);

        // Add jitter
        if (jitterFactor > 0) {
            double jitter = baseDelay * jitterFactor * ThreadLocalRandom.current().nextDouble();
            baseDelay += jitter;
        }

        return (long) baseDelay;
    }

    /**
     * Create default retry policy
     */
    public static ExponentialBackoffRetry defaultPolicy() {
        return ExponentialBackoffRetry.builder().build();
    }

    /**
     * Create retry policy with custom max attempts
     */
    public static ExponentialBackoffRetry withMaxAttempts(int maxAttempts) {
        return ExponentialBackoffRetry.builder()
                .maxAttempts(maxAttempts)
                .build();
    }

    /**
     * Create retry policy from configuration
     */
    public static ExponentialBackoffRetry fromConfig(int maxAttempts, long initialDelay,
                                                     double multiplier, long maxDelay) {
        return ExponentialBackoffRetry.builder()
                .maxAttempts(maxAttempts)
                .initialDelay(initialDelay)
                .multiplier(multiplier)
                .maxDelay(maxDelay)
                .build();
    }

    /**
     * Add non-retryable exception type
     */
    public ExponentialBackoffRetry addNonRetryable(Class<? extends Exception> exceptionClass) {
        this.nonRetryableExceptions.add(exceptionClass);
        return this;
    }

    /**
     * Calculate all delays for logging/debugging
     */
    public long[] getAllDelays() {
        long[] delays = new long[maxAttempts];
        for (int i = 0; i < maxAttempts; i++) {
            delays[i] = getNextDelay(i + 1);
        }
        return delays;
    }

    @Override
    public String toString() {
        return String.format("ExponentialBackoffRetry{maxAttempts=%d, initialDelay=%d, " +
                        "maxDelay=%d, multiplier=%.1f, jitterFactor=%.1f}",
                maxAttempts, initialDelay, maxDelay, multiplier, jitterFactor);
    }
}
