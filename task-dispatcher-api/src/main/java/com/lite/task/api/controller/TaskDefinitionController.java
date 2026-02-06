package com.lite.task.api.controller;

import com.lite.task.api.dto.response.TaskDefinitionResponse;
import com.lite.task.common.model.Result;
import com.lite.task.domain.task.entity.TaskDefinition;
import com.lite.task.infrastructure.persistence.repository.TaskDefinitionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

@RestController
@RequestMapping("/api/v1/task-definitions")
@RequiredArgsConstructor
@Tag(name = "Task Definition API", description = "Task definition query operations")
public class TaskDefinitionController {

    private final TaskDefinitionRepository taskDefinitionRepository;

    @GetMapping
    @Operation(summary = "List task definitions")
    public Result<List<TaskDefinitionResponse>> list() {
        List<TaskDefinitionResponse> definitions = taskDefinitionRepository
                .findAll(Sort.by(Sort.Direction.ASC, "taskType"))
                .stream()
                .map(this::toResponse)
                .toList();

        return Result.success(definitions);
    }

    private TaskDefinitionResponse toResponse(TaskDefinition definition) {
        return TaskDefinitionResponse.builder()
                .id(definition.getId())
                .taskType(definition.getTaskType())
                .taskName(definition.getTaskName())
                .executorType(definition.getExecutorType())
                .description(definition.getDescription())
                .timeoutSeconds(definition.getTimeoutSeconds())
                .rateLimit(definition.getRateLimit())
                .maxRetryAttempts(definition.getMaxRetryAttempts())
                .status(definition.getStatus())
                .createdAt(definition.getCreatedAt())
                .updatedAt(definition.getUpdatedAt())
                .build();
    }
}
