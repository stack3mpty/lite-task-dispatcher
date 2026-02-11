package com.lite.task.api.controller;

import com.lite.task.api.dto.request.TaskDefinitionCreateRequest;
import com.lite.task.api.dto.request.TaskDefinitionUpdateRequest;
import com.lite.task.api.dto.response.TaskDefinitionResponse;
import com.lite.task.common.exception.ErrorCode;
import com.lite.task.common.exception.TaskException;
import com.lite.task.common.model.Result;
import com.lite.task.domain.task.entity.TaskDefinition;
import com.lite.task.infrastructure.persistence.repository.TaskDefinitionRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Locale;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/task-definitions")
@RequiredArgsConstructor
@Tag(name = "Task Definition API", description = "Task definition management operations")
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

    @PostMapping
    @Transactional
    @Operation(summary = "Create task definition")
    public Result<TaskDefinitionResponse> create(@Valid @RequestBody TaskDefinitionCreateRequest request) {
        if (taskDefinitionRepository.existsByTaskType(request.getTaskType())) {
            throw new TaskException(ErrorCode.TASK_ALREADY_EXISTS,
                    "Task definition already exists: " + request.getTaskType());
        }

        TaskDefinition definition = TaskDefinition.builder()
                .taskType(request.getTaskType())
                .taskName(request.getTaskName())
                .executorType(request.getExecutorType())
                .executorConfig(normalizeMap(request.getExecutorConfig()))
                .retryPolicy(request.getRetryPolicy() != null ? request.getRetryPolicy().toMap() : defaultRetryPolicy())
                .description(request.getDescription())
                .timeoutSeconds(request.getTimeoutSeconds())
                .rateLimit(request.getRateLimit())
                .status(normalizeStatus(request.getStatus()))
                .build();

        TaskDefinition saved = taskDefinitionRepository.save(definition);
        return Result.success(toResponse(saved));
    }

    @PutMapping("/{id}")
    @Transactional
    @Operation(summary = "Update task definition runtime config")
    public Result<TaskDefinitionResponse> update(@PathVariable Long id,
                                                 @Valid @RequestBody TaskDefinitionUpdateRequest request) {
        TaskDefinition definition = getByIdOrThrow(id);
        definition.setTaskName(request.getTaskName());
        definition.setExecutorType(request.getExecutorType());
        definition.setExecutorConfig(normalizeMap(request.getExecutorConfig()));
        definition.setRetryPolicy(request.getRetryPolicy() != null ? request.getRetryPolicy().toMap() : defaultRetryPolicy());
        definition.setDescription(request.getDescription());
        definition.setTimeoutSeconds(request.getTimeoutSeconds());
        definition.setRateLimit(request.getRateLimit());

        TaskDefinition saved = taskDefinitionRepository.save(definition);
        return Result.success(toResponse(saved));
    }

    @PutMapping("/{id}/enable")
    @Transactional
    @Operation(summary = "Enable task definition")
    public Result<TaskDefinitionResponse> enable(@PathVariable Long id) {
        TaskDefinition definition = getByIdOrThrow(id);
        definition.enable();
        TaskDefinition saved = taskDefinitionRepository.save(definition);
        return Result.success(toResponse(saved));
    }

    @PutMapping("/{id}/disable")
    @Transactional
    @Operation(summary = "Disable task definition")
    public Result<TaskDefinitionResponse> disable(@PathVariable Long id) {
        TaskDefinition definition = getByIdOrThrow(id);
        definition.disable();
        TaskDefinition saved = taskDefinitionRepository.save(definition);
        return Result.success(toResponse(saved));
    }

    private TaskDefinitionResponse toResponse(TaskDefinition definition) {
        return TaskDefinitionResponse.builder()
                .id(definition.getId())
                .taskType(definition.getTaskType())
                .taskName(definition.getTaskName())
                .executorType(definition.getExecutorType())
                .executorConfig(definition.getExecutorConfig())
                .retryPolicy(definition.getRetryPolicy())
                .description(definition.getDescription())
                .timeoutSeconds(definition.getTimeoutSeconds())
                .rateLimit(definition.getRateLimit())
                .maxRetryAttempts(definition.getMaxRetryAttempts())
                .status(definition.getStatus())
                .createdAt(definition.getCreatedAt())
                .updatedAt(definition.getUpdatedAt())
                .build();
    }

    private TaskDefinition getByIdOrThrow(Long id) {
        return taskDefinitionRepository.findById(id)
                .orElseThrow(() -> new TaskException(ErrorCode.TASK_NOT_FOUND,
                        "Task definition not found: " + id));
    }

    private String normalizeStatus(String status) {
        if (status == null || status.isBlank()) {
            return "ENABLED";
        }
        String normalized = status.toUpperCase(Locale.ROOT);
        if (!"ENABLED".equals(normalized) && !"DISABLED".equals(normalized)) {
            throw new TaskException(ErrorCode.PARAM_INVALID, "Unsupported task definition status: " + status);
        }
        return normalized;
    }

    private Map<String, Object> normalizeMap(Map<String, Object> value) {
        return value != null ? value : Map.of();
    }

    private Map<String, Object> defaultRetryPolicy() {
        return Map.of(
                "maxAttempts", 3,
                "initialDelay", 1000,
                "multiplier", 2.0,
                "maxDelay", 60000
        );
    }
}
