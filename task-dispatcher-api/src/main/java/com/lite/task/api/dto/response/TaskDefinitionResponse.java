package com.lite.task.api.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskDefinitionResponse {

    private Long id;
    private String taskType;
    private String taskName;
    private String executorType;
    private Map<String, Object> executorConfig;
    private Map<String, Object> retryPolicy;
    private String description;
    private Integer timeoutSeconds;
    private Integer rateLimit;
    private Integer maxRetryAttempts;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
