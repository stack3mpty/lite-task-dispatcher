package com.lite.task.common.exception;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Error Code Definition
 *
 * Code format: XYYZZZ
 * - X: Category (1=Task, 2=Queue, 3=Executor, 4=System)
 * - YY: Sub-category
 * - ZZZ: Specific error
 *
 * @author lite-task-dispatcher
 */
@Getter
@AllArgsConstructor
public enum ErrorCode {

    // ==================== Success ====================
    SUCCESS(0, "Success"),

    // ==================== Task Errors (1XXXX) ====================
    TASK_NOT_FOUND(10001, "Task not found"),
    TASK_ALREADY_EXISTS(10002, "Task already exists"),
    TASK_INVALID_STATUS(10003, "Invalid task status for this operation"),
    TASK_INVALID_PARAMS(10004, "Invalid task parameters"),
    TASK_TYPE_NOT_FOUND(10005, "Task type not found"),
    TASK_CANCELLED(10006, "Task has been cancelled"),
    TASK_EXPIRED(10007, "Task has expired"),
    TASK_DUPLICATE(10008, "Duplicate task detected"),

    // ==================== Queue Errors (2XXXX) ====================
    QUEUE_FULL(20001, "Task queue is full"),
    QUEUE_EMPTY(20002, "Task queue is empty"),
    QUEUE_OPERATION_FAILED(20003, "Queue operation failed"),

    // ==================== Rate Limit Errors (21XXX) ====================
    RATE_LIMIT_EXCEEDED(21001, "Rate limit exceeded"),
    RATE_LIMIT_CONFIG_ERROR(21002, "Rate limit configuration error"),

    // ==================== Executor Errors (3XXXX) ====================
    EXECUTOR_NOT_FOUND(30001, "Executor not found for task type"),
    EXECUTOR_EXECUTION_FAILED(30002, "Task execution failed"),
    EXECUTOR_TIMEOUT(30003, "Task execution timeout"),
    EXECUTOR_RETRY_EXHAUSTED(30004, "Retry attempts exhausted"),

    // ==================== Lock Errors (31XXX) ====================
    LOCK_ACQUIRE_FAILED(31001, "Failed to acquire distributed lock"),
    LOCK_RELEASE_FAILED(31002, "Failed to release distributed lock"),

    // ==================== System Errors (4XXXX) ====================
    SYSTEM_ERROR(40001, "Internal system error"),
    DATABASE_ERROR(40002, "Database operation failed"),
    REDIS_ERROR(40003, "Redis operation failed"),
    KAFKA_ERROR(40004, "Kafka operation failed"),
    SERIALIZATION_ERROR(40005, "Serialization/Deserialization error"),

    // ==================== Validation Errors (5XXXX) ====================
    VALIDATION_ERROR(50001, "Validation error"),
    PARAM_MISSING(50002, "Required parameter is missing"),
    PARAM_INVALID(50003, "Parameter value is invalid");

    private final int code;
    private final String message;

    /**
     * Check if this is a success code
     */
    public boolean isSuccess() {
        return this == SUCCESS;
    }

    /**
     * Check if this is a client error (can be retried with different input)
     */
    public boolean isClientError() {
        return code >= 10000 && code < 40000;
    }

    /**
     * Check if this is a server error (may be retried)
     */
    public boolean isServerError() {
        return code >= 40000;
    }

    /**
     * Check if error is retryable
     */
    public boolean isRetryable() {
        return this == EXECUTOR_EXECUTION_FAILED
                || this == EXECUTOR_TIMEOUT
                || this == LOCK_ACQUIRE_FAILED
                || this == DATABASE_ERROR
                || this == REDIS_ERROR
                || this == KAFKA_ERROR;
    }
}
