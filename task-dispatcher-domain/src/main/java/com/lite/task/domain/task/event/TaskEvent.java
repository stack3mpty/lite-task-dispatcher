package com.lite.task.domain.task.event;

import lombok.Data;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Base Task Event
 *
 * Base class for all task-related domain events
 *
 * @author lite-task-dispatcher
 */
@Data
public abstract class TaskEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Unique event ID
     */
    private final String eventId;

    /**
     * Task ID that triggered this event
     */
    private final String taskId;

    /**
     * Task type
     */
    private final String taskType;

    /**
     * Event timestamp
     */
    private final LocalDateTime timestamp;

    /**
     * Event source (service/component that generated this event)
     */
    private String source;

    /**
     * Trace ID for distributed tracing
     */
    private String traceId;

    protected TaskEvent(String taskId, String taskType) {
        this.eventId = UUID.randomUUID().toString().replace("-", "");
        this.taskId = taskId;
        this.taskType = taskType;
        this.timestamp = LocalDateTime.now();
    }

    /**
     * Get event type name
     */
    public abstract String getEventType();
}
