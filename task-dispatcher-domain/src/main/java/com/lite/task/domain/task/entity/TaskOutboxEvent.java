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
 * Task Outbox Event.
 *
 * Stores reliable dispatch intents so Redis/Kafka failures can be retried.
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "task_outbox_event", indexes = {
        @Index(name = "idx_task_outbox_status_next_retry", columnList = "status, nextRetryAt"),
        @Index(name = "idx_task_outbox_task_id", columnList = "taskId")
})
public class TaskOutboxEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String EVENT_TASK_SUBMITTED = "TASK_SUBMITTED";
    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_RETRY = "RETRY";
    public static final String STATUS_PUBLISHED = "PUBLISHED";
    public static final String STATUS_FAILED = "FAILED";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "task_id", nullable = false, length = 32, unique = true)
    private String taskId;

    @Type(JsonType.class)
    @Column(name = "payload", columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @Column(name = "status", nullable = false, length = 16)
    @Builder.Default
    private String status = STATUS_NEW;

    @Column(name = "retry_count", nullable = false)
    @Builder.Default
    private Integer retryCount = 0;

    @Column(name = "next_retry_at")
    private LocalDateTime nextRetryAt;

    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;
        if (this.nextRetryAt == null) {
            this.nextRetryAt = now;
        }
        if (this.status == null || this.status.isBlank()) {
            this.status = STATUS_NEW;
        }
        if (this.retryCount == null) {
            this.retryCount = 0;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    public void markPublished() {
        this.status = STATUS_PUBLISHED;
        this.lastError = null;
        this.nextRetryAt = null;
    }

    public void markRetry(String error, int maxRetryCount, long baseRetryDelayMs) {
        this.retryCount = this.retryCount + 1;
        this.lastError = error;
        if (this.retryCount > maxRetryCount) {
            this.status = STATUS_FAILED;
            this.nextRetryAt = null;
            return;
        }
        this.status = STATUS_RETRY;
        long delayMs = Math.max(baseRetryDelayMs, 1000L) * this.retryCount;
        this.nextRetryAt = LocalDateTime.now().plusNanos(delayMs * 1_000_000L);
    }

    public boolean isPublishable() {
        return STATUS_NEW.equals(this.status) || STATUS_RETRY.equals(this.status);
    }
}

