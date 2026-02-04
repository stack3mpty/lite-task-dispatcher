package com.lite.task.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Task Status Enum
 *
 * State transitions:
 * CREATED -> PENDING -> RUNNING -> SUCCESS/FAILED
 * FAILED -> RETRYING -> RUNNING (retry)
 * FAILED/RETRYING -> DEAD (max retry exceeded)
 * PENDING -> CANCELLED (user cancel)
 *
 * @author lite-task-dispatcher
 */
@Getter
@AllArgsConstructor
public enum TaskStatus {

    /**
     * Task created, not yet submitted to queue
     */
    CREATED("CREATED", "Created"),

    /**
     * Task submitted to queue, waiting for execution
     */
    PENDING("PENDING", "Pending"),

    /**
     * Task is being executed
     */
    RUNNING("RUNNING", "Running"),

    /**
     * Task executed successfully
     */
    SUCCESS("SUCCESS", "Success"),

    /**
     * Task execution failed
     */
    FAILED("FAILED", "Failed"),

    /**
     * Task is being retried
     */
    RETRYING("RETRYING", "Retrying"),

    /**
     * Task reached max retry limit, marked as dead
     */
    DEAD("DEAD", "Dead"),

    /**
     * Task cancelled by user
     */
    CANCELLED("CANCELLED", "Cancelled");

    private final String code;
    private final String description;

    /**
     * Check if task is in terminal state (no more transitions possible)
     */
    public boolean isTerminal() {
        return this == SUCCESS || this == DEAD || this == CANCELLED;
    }

    /**
     * Check if task can be cancelled
     */
    public boolean isCancellable() {
        return this == CREATED || this == PENDING;
    }

    /**
     * Check if task can be retried
     */
    public boolean isRetryable() {
        return this == FAILED;
    }

    public static TaskStatus fromCode(String code) {
        for (TaskStatus status : values()) {
            if (status.getCode().equalsIgnoreCase(code)) {
                return status;
            }
        }
        throw new IllegalArgumentException("Unknown task status: " + code);
    }
}
