package com.lite.task.core.dispatcher;

import com.lite.task.domain.task.entity.TaskExecutionLog;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.infrastructure.persistence.repository.TaskExecutionLogRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

/**
 * Async persistence service to avoid self-invocation issues on @Async methods.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TaskPersistenceAsyncService {

    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskExecutionLogRepository executionLogRepository;

    @Async("taskExecutor")
    public void persistTask(TaskInstance task) {
        try {
            if (task.getId() == null) {
                taskInstanceRepository.findByTaskId(task.getTaskId()).ifPresent(existing -> task.setId(existing.getId()));
            }

            if (task.getId() == null) {
                taskInstanceRepository.save(task);
            } else {
                taskInstanceRepository.findById(task.getId()).ifPresentOrElse(existing -> {
                    existing.setStatus(task.getStatus());
                    existing.setResult(task.getResult());
                    existing.setErrorMessage(task.getErrorMessage());
                    existing.setRetryCount(task.getRetryCount());
                    existing.setStartedAt(task.getStartedAt());
                    existing.setFinishedAt(task.getFinishedAt());
                    existing.setExecutorId(task.getExecutorId());
                    existing.setUpdatedAt(LocalDateTime.now());
                    taskInstanceRepository.save(existing);
                }, () -> taskInstanceRepository.save(task));
            }
            log.debug("Task persisted to database: taskId={}", task.getTaskId());
        } catch (Exception e) {
            log.error("Failed to persist task to database (non-blocking): taskId={}, error={}",
                    task.getTaskId(), e.getMessage(), e);
        }
    }

    @Async("taskExecutor")
    public void persistExecutionLog(TaskExecutionLog logEntry) {
        try {
            executionLogRepository.save(logEntry);
            log.debug("Execution log persisted: taskId={}", logEntry.getTaskId());
        } catch (Exception e) {
            log.error("Failed to persist execution log (non-blocking): taskId={}, error={}",
                    logEntry.getTaskId(), e.getMessage(), e);
        }
    }
}

