package com.lite.task.common.exception;

import lombok.Getter;

/**
 * Base Task Exception
 *
 * @author lite-task-dispatcher
 */
@Getter
public class TaskException extends RuntimeException {

    private final ErrorCode errorCode;
    private final Object[] args;

    public TaskException(ErrorCode errorCode) {
        super(errorCode.getMessage());
        this.errorCode = errorCode;
        this.args = null;
    }

    public TaskException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
        this.args = null;
    }

    public TaskException(ErrorCode errorCode, Object... args) {
        super(formatMessage(errorCode.getMessage(), args));
        this.errorCode = errorCode;
        this.args = args;
    }

    public TaskException(ErrorCode errorCode, Throwable cause) {
        super(errorCode.getMessage(), cause);
        this.errorCode = errorCode;
        this.args = null;
    }

    public TaskException(ErrorCode errorCode, Throwable cause, Object... args) {
        super(formatMessage(errorCode.getMessage(), args), cause);
        this.errorCode = errorCode;
        this.args = args;
    }

    public int getCode() {
        return errorCode.getCode();
    }

    /**
     * Check if this exception is retryable
     */
    public boolean isRetryable() {
        return errorCode.isRetryable();
    }

    private static String formatMessage(String template, Object[] args) {
        if (args == null || args.length == 0) {
            return template;
        }
        return String.format(template.replace("{}", "%s"), args);
    }

    /**
     * Factory methods for common exceptions
     */
    public static TaskException notFound(String taskId) {
        return new TaskException(ErrorCode.TASK_NOT_FOUND, "Task not found: " + taskId);
    }

    public static TaskException invalidStatus(String taskId, String currentStatus, String expectedStatus) {
        return new TaskException(ErrorCode.TASK_INVALID_STATUS,
                String.format("Task %s status is %s, expected %s", taskId, currentStatus, expectedStatus));
    }

    public static TaskException duplicate(String taskId) {
        return new TaskException(ErrorCode.TASK_DUPLICATE, "Duplicate task: " + taskId);
    }

    public static TaskException executorNotFound(String executorType) {
        return new TaskException(ErrorCode.EXECUTOR_NOT_FOUND, "Executor not found: " + executorType);
    }

    public static TaskException timeout(String taskId) {
        return new TaskException(ErrorCode.EXECUTOR_TIMEOUT, "Task timeout: " + taskId);
    }
}
