package com.lite.task.api.controller;

import com.lite.task.api.dto.request.TaskSubmitRequest;
import com.lite.task.api.dto.response.TaskResponse;
import com.lite.task.common.model.PageResult;
import com.lite.task.common.model.Result;
import com.lite.task.core.producer.TaskProducer;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.web.bind.annotation.*;

/**
 * Task Controller
 *
 * REST API for task operations
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
@Tag(name = "Task API", description = "Task management operations")
public class TaskController {

    private final TaskProducer taskProducer;
    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskCacheOperator taskCacheOperator;

    /**
     * Submit a new task
     */
    @PostMapping
    @Operation(summary = "Submit a new task")
    public Result<TaskResponse> submit(@Valid @RequestBody TaskSubmitRequest request) {
        log.info("Submitting task: type={}", request.getTaskType());

        TaskInstance task = taskProducer.submit(
                request.getTaskType(),
                request.getParams(),
                request.getPriority() != null ? request.getPriority() : 2,
                request.getExecuteAt(),
                request.getCallbackUrl(),
                request.getCreatedBy()
        );

        return Result.success(toResponse(task));
    }

    /**
     * Get task by ID
     * Priority: Redis (active tasks) -> DB (historical tasks)
     */
    @GetMapping("/{taskId}")
    @Operation(summary = "Get task by ID")
    public Result<TaskResponse> getById(@PathVariable String taskId) {
        // Try Redis first (primary storage for active tasks)
        TaskInstance task = taskCacheOperator.get(taskId);

        // Fallback to DB for historical tasks
        if (task == null) {
            task = taskInstanceRepository.findByTaskId(taskId).orElse(null);
        }

        if (task == null) {
            return Result.failure(10001, "Task not found: " + taskId);
        }

        return Result.success(toResponse(task));
    }

    /**
     * List tasks with pagination
     */
    @GetMapping
    @Operation(summary = "List tasks with pagination")
    public PageResult<TaskResponse> list(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String taskType,
            @RequestParam(required = false) String status) {

        PageRequest pageRequest = PageRequest.of(page - 1, size, Sort.by(Sort.Direction.DESC, "createdAt"));

        Page<TaskInstance> taskPage;
        if (taskType != null && !taskType.isEmpty()) {
            taskPage = taskInstanceRepository.findByTaskType(taskType, pageRequest);
        } else {
            taskPage = taskInstanceRepository.findAll(pageRequest);
        }

        return PageResult.of(
                taskPage.getContent().stream().map(this::toResponse).toList(),
                page,
                size,
                taskPage.getTotalElements()
        );
    }

    /**
     * Cancel a task
     */
    @PutMapping("/{taskId}/cancel")
    @Operation(summary = "Cancel a pending task")
    public Result<Void> cancel(@PathVariable String taskId) {
        log.info("Cancelling task: {}", taskId);
        taskProducer.cancel(taskId);
        return Result.success();
    }

    /**
     * Retry a failed task
     * Priority: Redis (active tasks) -> DB (historical tasks)
     */
    @PutMapping("/{taskId}/retry")
    @Operation(summary = "Retry a failed task")
    public Result<TaskResponse> retry(@PathVariable String taskId) {
        log.info("Retrying task: {}", taskId);

        // Try Redis first (primary storage)
        TaskInstance task = taskCacheOperator.get(taskId);

        // Fallback to DB for historical tasks
        if (task == null) {
            task = taskInstanceRepository.findByTaskId(taskId).orElse(null);
        }

        if (task == null) {
            return Result.failure(10001, "Task not found: " + taskId);
        }

        if (!task.getStatus().isRetryable()) {
            return Result.failure(10003, "Task cannot be retried in current status: " + task.getStatus());
        }

        // Schedule retry
        task.scheduleRetry();

        // Update Redis (primary storage)
        taskCacheOperator.save(task);

        // Async persist to DB
        taskInstanceRepository.save(task);

        return Result.success(toResponse(task));
    }

    private TaskResponse toResponse(TaskInstance task) {
        return TaskResponse.builder()
                .taskId(task.getTaskId())
                .taskType(task.getTaskType())
                .status(task.getStatus())
                .priority(task.getPriority())
                .params(task.getParams())
                .result(task.getResult())
                .errorMessage(task.getErrorMessage())
                .retryCount(task.getRetryCount())
                .maxRetry(task.getMaxRetry())
                .executeAt(task.getExecuteAt())
                .startedAt(task.getStartedAt())
                .finishedAt(task.getFinishedAt())
                .callbackUrl(task.getCallbackUrl())
                .createdBy(task.getCreatedBy())
                .executorId(task.getExecutorId())
                .createdAt(task.getCreatedAt())
                .updatedAt(task.getUpdatedAt())
                .durationMs(task.getDurationMs())
                .build();
    }
}
