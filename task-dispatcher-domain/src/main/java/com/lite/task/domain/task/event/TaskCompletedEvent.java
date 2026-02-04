package com.lite.task.domain.task.event;

import lombok.Getter;

import java.util.Map;

/**
 * Task Completed Event
 *
 * Published when a task completes successfully
 *
 * @author lite-task-dispatcher
 */
@Getter
public class TaskCompletedEvent extends TaskEvent {

    private static final String EVENT_TYPE = "TASK_COMPLETED";

    private final Map<String, Object> result;
    private final long durationMs;
    private final String executorId;
    private final int attemptNumber;

    public TaskCompletedEvent(String taskId, String taskType, Map<String, Object> result,
                              long durationMs, String executorId, int attemptNumber) {
        super(taskId, taskType);
        this.result = result;
        this.durationMs = durationMs;
        this.executorId = executorId;
        this.attemptNumber = attemptNumber;
    }

    @Override
    public String getEventType() {
        return EVENT_TYPE;
    }
}
