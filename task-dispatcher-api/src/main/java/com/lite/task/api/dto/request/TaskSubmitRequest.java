package com.lite.task.api.dto.request;

import jakarta.validation.constraints.*;
import lombok.Data;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Task Submit Request DTO
 *
 * @author lite-task-dispatcher
 */
@Data
public class TaskSubmitRequest {

    /**
     * Task type (must be registered)
     */
    @NotBlank(message = "Task type is required")
    @Size(max = 64, message = "Task type must be at most 64 characters")
    private String taskType;

    /**
     * Task priority (0-4, 0 is highest)
     */
    @Min(value = 0, message = "Priority must be between 0 and 4")
    @Max(value = 4, message = "Priority must be between 0 and 4")
    private Integer priority = 2;

    /**
     * Task input parameters
     */
    private Map<String, Object> params;

    /**
     * Scheduled execution time (null for immediate)
     */
    private LocalDateTime executeAt;

    /**
     * Callback URL for result notification
     */
    @Size(max = 512, message = "Callback URL must be at most 512 characters")
    private String callbackUrl;

    /**
     * Creator identifier
     */
    @Size(max = 64, message = "Created by must be at most 64 characters")
    private String createdBy;
}
