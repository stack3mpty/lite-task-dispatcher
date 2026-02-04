package com.lite.task.domain.task.entity;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
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
 * Task Instance Entity
 *
 * Represents a single task execution instance
 *
 * @author lite-task-dispatcher
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "task_instance", indexes = {
        @Index(name = "idx_task_instance_task_id", columnList = "taskId"),
        @Index(name = "idx_task_instance_status", columnList = "status"),
        @Index(name = "idx_task_instance_priority", columnList = "priority"),
        @Index(name = "idx_task_instance_execute_at", columnList = "executeAt"),
        @Index(name = "idx_task_instance_created_at", columnList = "createdAt"),
        @Index(name = "idx_task_instance_status_priority", columnList = "status, priority"),
        @Index(name = "idx_task_instance_def_status", columnList = "taskDefId, status")
})
public class TaskInstance implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Unique task ID (Snowflake algorithm generated)
     */
    @Column(name = "task_id", nullable = false, unique = true, length = 32)
    private String taskId;

    /**
     * Reference to task definition
     */
    @Column(name = "task_def_id", nullable = false)
    private Long taskDefId;

    /**
     * Task type (denormalized for query efficiency)
     */
    @Column(name = "task_type", nullable = false, length = 64)
    private String taskType;

    /**
     * Current task status
     */
    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 16)
    @Builder.Default
    private TaskStatus status = TaskStatus.CREATED;

    /**
     * Task priority (0 = highest, 4 = lowest)
     */
    @Column(name = "priority", nullable = false)
    @Builder.Default
    private Integer priority = TaskPriority.DEFAULT.getLevel();

    /**
     * Task input parameters (JSON)
     */
    @Type(JsonType.class)
    @Column(name = "params", columnDefinition = "jsonb")
    private Map<String, Object> params;

    /**
     * Task execution result (JSON)
     */
    @Type(JsonType.class)
    @Column(name = "result", columnDefinition = "jsonb")
    private Map<String, Object> result;

    /**
     * Error message if failed
     */
    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    /**
     * Current retry count
     */
    @Column(name = "retry_count")
    @Builder.Default
    private Integer retryCount = 0;

    /**
     * Maximum retry attempts
     */
    @Column(name = "max_retry")
    @Builder.Default
    private Integer maxRetry = 3;

    /**
     * Scheduled execution time (for delayed tasks)
     */
    @Column(name = "execute_at")
    private LocalDateTime executeAt;

    /**
     * Actual start time
     */
    @Column(name = "started_at")
    private LocalDateTime startedAt;

    /**
     * Actual finish time
     */
    @Column(name = "finished_at")
    private LocalDateTime finishedAt;

    /**
     * Callback URL for result notification
     */
    @Column(name = "callback_url", length = 512)
    private String callbackUrl;

    /**
     * Creator identifier
     */
    @Column(name = "created_by", length = 64)
    private String createdBy;

    /**
     * Executor identifier (which worker executed this task)
     */
    @Column(name = "executor_id", length = 64)
    private String executorId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;

        // Set default execute time if not specified (immediate execution)
        if (this.executeAt == null) {
            this.executeAt = now;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    // ==================== State Transition Methods ====================

    /**
     * Submit task to pending queue
     */
    public void submit() {
        if (this.status != TaskStatus.CREATED) {
            throw new IllegalStateException(
                    String.format("Cannot submit task in status %s, expected CREATED", this.status));
        }
        this.status = TaskStatus.PENDING;
    }

    /**
     * Start task execution
     */
    public void start(String executorId) {
        if (this.status != TaskStatus.PENDING && this.status != TaskStatus.RETRYING) {
            throw new IllegalStateException(
                    String.format("Cannot start task in status %s, expected PENDING or RETRYING", this.status));
        }
        this.status = TaskStatus.RUNNING;
        this.startedAt = LocalDateTime.now();
        this.executorId = executorId;
    }

    /**
     * Complete task successfully
     */
    public void complete(Map<String, Object> result) {
        if (this.status != TaskStatus.RUNNING) {
            throw new IllegalStateException(
                    String.format("Cannot complete task in status %s, expected RUNNING", this.status));
        }
        this.status = TaskStatus.SUCCESS;
        this.result = result;
        this.finishedAt = LocalDateTime.now();
    }

    /**
     * Mark task as failed
     */
    public void fail(String errorMessage) {
        if (this.status != TaskStatus.RUNNING) {
            throw new IllegalStateException(
                    String.format("Cannot fail task in status %s, expected RUNNING", this.status));
        }
        this.status = TaskStatus.FAILED;
        this.errorMessage = errorMessage;
        this.finishedAt = LocalDateTime.now();
    }

    /**
     * Schedule retry
     *
     * @return true if retry scheduled, false if max retries exceeded
     */
    public boolean scheduleRetry() {
        if (this.status != TaskStatus.FAILED) {
            throw new IllegalStateException(
                    String.format("Cannot retry task in status %s, expected FAILED", this.status));
        }

        if (this.retryCount >= this.maxRetry) {
            this.status = TaskStatus.DEAD;
            return false;
        }

        this.retryCount++;
        this.status = TaskStatus.RETRYING;
        this.startedAt = null;
        this.finishedAt = null;
        this.executorId = null;
        return true;
    }

    /**
     * Cancel task
     */
    public void cancel() {
        if (!this.status.isCancellable()) {
            throw new IllegalStateException(
                    String.format("Cannot cancel task in status %s", this.status));
        }
        this.status = TaskStatus.CANCELLED;
        this.finishedAt = LocalDateTime.now();
    }

    /**
     * Mark task as dead (max retries exceeded)
     */
    public void markAsDead() {
        this.status = TaskStatus.DEAD;
        this.finishedAt = LocalDateTime.now();
    }

    // ==================== Query Methods ====================

    /**
     * Check if task is in terminal state
     */
    public boolean isTerminal() {
        return this.status.isTerminal();
    }

    /**
     * Check if task is delayed (scheduled for future)
     */
    public boolean isDelayed() {
        return this.executeAt != null && this.executeAt.isAfter(LocalDateTime.now());
    }

    /**
     * Check if task is ready for execution
     */
    public boolean isReady() {
        return this.status == TaskStatus.PENDING && !isDelayed();
    }

    /**
     * Get execution duration in milliseconds
     */
    public Long getDurationMs() {
        if (this.startedAt == null || this.finishedAt == null) {
            return null;
        }
        return java.time.Duration.between(this.startedAt, this.finishedAt).toMillis();
    }

    /**
     * Get task priority enum
     */
    public TaskPriority getPriorityEnum() {
        return TaskPriority.fromLevel(this.priority);
    }
}
