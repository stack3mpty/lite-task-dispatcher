package com.lite.task.api.controller;

import com.lite.task.api.dto.request.TaskSubmitRequest;
import com.lite.task.api.dto.response.TaskResponse;
import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.exception.TaskException;
import com.lite.task.common.model.PageResult;
import com.lite.task.common.model.Result;
import com.lite.task.core.producer.TaskProducer;
import com.lite.task.domain.task.entity.TaskExecutionLog;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.infrastructure.persistence.repository.TaskExecutionLogRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
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
    private final TaskExecutionLogRepository taskExecutionLogRepository;
    private final TaskCacheOperator taskCacheOperator;
    private final TaskQueueOperator taskQueueOperator;

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
        TaskInstance cacheTask = taskCacheOperator.get(taskId);
        TaskInstance dbTask = taskInstanceRepository.findByTaskId(taskId).orElse(null);
        TaskInstance task = mergeForRead(cacheTask, dbTask);

        if (task == null) {
            throw TaskException.notFound(taskId);
        }

        if (task.getResult() == null) {
            task.setResult(getLatestResult(taskId));
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

        TaskStatus taskStatus = null;
        if (status != null && !status.isEmpty()) {
            try {
                taskStatus = TaskStatus.fromCode(status);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid status filter: {}", status);
            }
        }

        Page<TaskInstance> taskPage;
        if (taskType != null && !taskType.isEmpty() && taskStatus != null) {
            taskPage = taskInstanceRepository.findByTaskTypeAndStatus(taskType, taskStatus, pageRequest);
        } else if (taskType != null && !taskType.isEmpty()) {
            taskPage = taskInstanceRepository.findByTaskType(taskType, pageRequest);
        } else if (taskStatus != null) {
            taskPage = taskInstanceRepository.findByStatus(taskStatus, pageRequest);
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
            throw TaskException.notFound(taskId);
        }

        if (!task.getStatus().isRetryable()) {
            throw TaskException.invalidStatus(taskId, task.getStatus().getCode(), "FAILED");
        }

        // Schedule retry
        task.scheduleRetry();

        // Update Redis (primary storage) and enqueue for execution
        taskCacheOperator.updateRetry(taskId, task.getRetryCount(), TaskStatus.RETRYING);
        TaskPriority priority = task.getPriorityEnum() != null ? task.getPriorityEnum() : TaskPriority.DEFAULT;
        taskQueueOperator.push(taskId, priority);

        // Persist to DB
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

    private TaskInstance mergeForRead(TaskInstance cacheTask, TaskInstance dbTask) {
        if (cacheTask == null) {
            return dbTask;
        }
        if (dbTask == null) {
            return cacheTask;
        }

        // Redis does not persist result payload, DB is source of truth for terminal tasks.
        if (cacheTask.getStatus() != null && cacheTask.getStatus().isTerminal()) {
            return dbTask;
        }

        if (cacheTask.getResult() == null) {
            cacheTask.setResult(dbTask.getResult());
        }
        if (isBlank(cacheTask.getErrorMessage()) && !isBlank(dbTask.getErrorMessage())) {
            cacheTask.setErrorMessage(dbTask.getErrorMessage());
        }
        if (cacheTask.getStartedAt() == null) {
            cacheTask.setStartedAt(dbTask.getStartedAt());
        }
        if (cacheTask.getFinishedAt() == null) {
            cacheTask.setFinishedAt(dbTask.getFinishedAt());
        }
        return cacheTask;
    }

    private java.util.Map<String, Object> getLatestResult(String taskId) {
        java.util.List<TaskExecutionLog> latestLogs = taskExecutionLogRepository.findLatestByTaskId(
                taskId, PageRequest.of(0, 1)
        );
        if (latestLogs == null || latestLogs.isEmpty()) {
            return null;
        }
        return latestLogs.get(0).getResult();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
