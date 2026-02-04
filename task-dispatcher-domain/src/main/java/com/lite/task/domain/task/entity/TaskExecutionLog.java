package com.lite.task.domain.task.entity;

import io.hypersistence.utils.hibernate.type.json.JsonType;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Map;

/**
 * Task Execution Log Entity
 *
 * Records each execution attempt of a task
 *
 * @author lite-task-dispatcher
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "task_execution_log", indexes = {
        @Index(name = "idx_task_log_instance_id", columnList = "taskInstanceId"),
        @Index(name = "idx_task_log_created_at", columnList = "createdAt")
})
public class TaskExecutionLog implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Reference to task instance
     */
    @Column(name = "task_instance_id", nullable = false)
    private Long taskInstanceId;

    /**
     * Task ID (denormalized for convenience)
     */
    @Column(name = "task_id", nullable = false, length = 32)
    private String taskId;

    /**
     * Attempt number (1-based)
     */
    @Column(name = "attempt_number", nullable = false)
    private Integer attemptNumber;

    /**
     * Execution status: SUCCESS / FAILED / TIMEOUT
     */
    @Column(name = "status", nullable = false, length = 16)
    private String status;

    /**
     * Execution message (success info or error message)
     */
    @Column(name = "message", columnDefinition = "text")
    private String message;

    /**
     * Detailed error information (stack trace, etc.)
     */
    @Column(name = "error_detail", columnDefinition = "text")
    private String errorDetail;

    /**
     * Execution result (JSON) - stored here since Redis doesn't store result
     */
    @Type(JsonType.class)
    @Column(name = "result", columnDefinition = "jsonb")
    private Map<String, Object> result;

    /**
     * Execution duration in milliseconds
     */
    @Column(name = "duration_ms")
    private Long durationMs;

    /**
     * Executor IP address
     */
    @Column(name = "executor_ip", length = 45)
    private String executorIp;

    /**
     * Executor identifier
     */
    @Column(name = "executor_id", length = 64)
    private String executorId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @PrePersist
    protected void onCreate() {
        this.createdAt = LocalDateTime.now();
    }

    // ==================== Factory Methods ====================

    /**
     * Create success log
     */
    public static TaskExecutionLog success(TaskInstance task, int attemptNumber,
                                           long durationMs, String executorIp, String executorId) {
        return TaskExecutionLog.builder()
                .taskInstanceId(task.getId())
                .taskId(task.getTaskId())
                .attemptNumber(attemptNumber)
                .status("SUCCESS")
                .message("Task executed successfully")
                .durationMs(durationMs)
                .executorIp(executorIp)
                .executorId(executorId)
                .build();
    }

    /**
     * Create failure log
     */
    public static TaskExecutionLog failure(TaskInstance task, int attemptNumber,
                                           String errorMessage, String errorDetail,
                                           long durationMs, String executorIp, String executorId) {
        return TaskExecutionLog.builder()
                .taskInstanceId(task.getId())
                .taskId(task.getTaskId())
                .attemptNumber(attemptNumber)
                .status("FAILED")
                .message(errorMessage)
                .errorDetail(errorDetail)
                .durationMs(durationMs)
                .executorIp(executorIp)
                .executorId(executorId)
                .build();
    }

    /**
     * Create timeout log
     */
    public static TaskExecutionLog timeout(TaskInstance task, int attemptNumber,
                                           long durationMs, String executorIp, String executorId) {
        return TaskExecutionLog.builder()
                .taskInstanceId(task.getId())
                .taskId(task.getTaskId())
                .attemptNumber(attemptNumber)
                .status("TIMEOUT")
                .message("Task execution timeout")
                .durationMs(durationMs)
                .executorIp(executorIp)
                .executorId(executorId)
                .build();
    }

    /**
     * Check if this log represents a successful execution
     */
    public boolean isSuccess() {
        return "SUCCESS".equals(this.status);
    }

    /**
     * Check if this log represents a failed execution
     */
    public boolean isFailed() {
        return "FAILED".equals(this.status);
    }

    /**
     * Check if this log represents a timeout
     */
    public boolean isTimeout() {
        return "TIMEOUT".equals(this.status);
    }
}
