package com.lite.task.api.dto.request;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Data;

import java.util.HashMap;
import java.util.Map;

/**
 * Task definition update request DTO.
 */
@Data
public class TaskDefinitionUpdateRequest {

    @NotBlank(message = "taskName is required")
    @Size(max = 128, message = "taskName must be at most 128 characters")
    private String taskName;

    @NotBlank(message = "executorType is required")
    @Size(max = 64, message = "executorType must be at most 64 characters")
    private String executorType;

    @Size(max = 2000, message = "description must be at most 2000 characters")
    private String description;

    @NotNull(message = "timeoutSeconds is required")
    @Min(value = 1, message = "timeoutSeconds must be >= 1")
    private Integer timeoutSeconds = 60;

    @NotNull(message = "rateLimit is required")
    @Min(value = 1, message = "rateLimit must be >= 1")
    private Integer rateLimit = 100;

    private Map<String, Object> executorConfig = new HashMap<>();

    @Valid
    private RetryPolicyRequest retryPolicy = new RetryPolicyRequest();
}
