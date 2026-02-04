package com.lite.task.domain.task.event;

import lombok.Getter;

/**
 * Task Failed Event
 *
 * Published when a task execution fails
 *
 * @author lite-task-dispatcher
 */
@Getter
public class TaskFailedEvent extends TaskEvent {

    private static final String EVENT_TYPE = "TASK_FAILED";

    private final String errorMessage;
    private final String errorDetail;
    private final long durationMs;
    private final String executorId;
    private final int attemptNumber;
    private final boolean willRetry;
    private final boolean isDead;

    public TaskFailedEvent(String taskId, String taskType, String errorMessage, String errorDetail,
                          long durationMs, String executorId, int attemptNumber,
                          boolean willRetry, boolean isDead) {
        super(taskId, taskType);
        this.errorMessage = errorMessage;
        this.errorDetail = errorDetail;
        this.durationMs = durationMs;
        this.executorId = executorId;
        this.attemptNumber = attemptNumber;
        this.willRetry = willRetry;
        this.isDead = isDead;
    }

    @Override
    public String getEventType() {
        return EVENT_TYPE;
    }

    /**
     * Create event for failed task that will be retried
     */
    public static TaskFailedEvent withRetry(String taskId, String taskType, String errorMessage,
                                            String errorDetail, long durationMs, String executorId,
                                            int attemptNumber) {
        return new TaskFailedEvent(taskId, taskType, errorMessage, errorDetail,
                durationMs, executorId, attemptNumber, true, false);
    }

    /**
     * Create event for failed task that is marked as dead
     */
    public static TaskFailedEvent dead(String taskId, String taskType, String errorMessage,
                                       String errorDetail, long durationMs, String executorId,
                                       int attemptNumber) {
        return new TaskFailedEvent(taskId, taskType, errorMessage, errorDetail,
                durationMs, executorId, attemptNumber, false, true);
    }

    /**
     * Create event for failed task that won't be retried (non-retryable error)
     */
    public static TaskFailedEvent permanent(String taskId, String taskType, String errorMessage,
                                            String errorDetail, long durationMs, String executorId,
                                            int attemptNumber) {
        return new TaskFailedEvent(taskId, taskType, errorMessage, errorDetail,
                durationMs, executorId, attemptNumber, false, false);
    }
}
