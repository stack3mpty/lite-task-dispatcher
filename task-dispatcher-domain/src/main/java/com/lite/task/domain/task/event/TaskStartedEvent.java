package com.lite.task.domain.task.event;

import lombok.Getter;

/**
 * Task Started Event
 *
 * Published when a task starts execution
 *
 * @author lite-task-dispatcher
 */
@Getter
public class TaskStartedEvent extends TaskEvent {

    private static final String EVENT_TYPE = "TASK_STARTED";

    private final String executorId;
    private final int attemptNumber;

    public TaskStartedEvent(String taskId, String taskType, String executorId, int attemptNumber) {
        super(taskId, taskType);
        this.executorId = executorId;
        this.attemptNumber = attemptNumber;
    }

    @Override
    public String getEventType() {
        return EVENT_TYPE;
    }
}
