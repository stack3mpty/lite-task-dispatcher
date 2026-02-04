package com.lite.task.api.dto.response;

import com.lite.task.common.enums.TaskStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Task Response DTO
 *
 * @author lite-task-dispatcher
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskResponse {

    private String taskId;
    private String taskType;
    private TaskStatus status;
    private Integer priority;
    private Map<String, Object> params;
    private Map<String, Object> result;
    private String errorMessage;
    private Integer retryCount;
    private Integer maxRetry;
    private LocalDateTime executeAt;
    private LocalDateTime startedAt;
    private LocalDateTime finishedAt;
    private String callbackUrl;
    private String createdBy;
    private String executorId;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private Long durationMs;
}
