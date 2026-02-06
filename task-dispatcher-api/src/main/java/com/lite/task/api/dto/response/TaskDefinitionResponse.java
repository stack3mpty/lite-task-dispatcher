package com.lite.task.api.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskDefinitionResponse {

    private Long id;
    private String taskType;
    private String taskName;
    private String executorType;
    private String description;
    private Integer timeoutSeconds;
    private Integer rateLimit;
    private Integer maxRetryAttempts;
    private String status;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
