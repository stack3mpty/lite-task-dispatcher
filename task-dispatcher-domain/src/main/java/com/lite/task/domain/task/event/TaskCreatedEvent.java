package com.lite.task.domain.task.event;

import lombok.Getter;

import java.util.Map;

/**
 * Task Created Event
 *
 * Published when a new task is created
 *
 * @author lite-task-dispatcher
 */
@Getter
public class TaskCreatedEvent extends TaskEvent {

    private static final String EVENT_TYPE = "TASK_CREATED";

    private final int priority;
    private final Map<String, Object> params;
    private final String createdBy;

    public TaskCreatedEvent(String taskId, String taskType, int priority,
                           Map<String, Object> params, String createdBy) {
        super(taskId, taskType);
        this.priority = priority;
        this.params = params;
        this.createdBy = createdBy;
    }

    @Override
    public String getEventType() {
        return EVENT_TYPE;
    }
}
