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
 * Task Definition Entity
 *
 * Defines task types and their configurations
 *
 * @author lite-task-dispatcher
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "task_definition", indexes = {
        @Index(name = "idx_task_def_type", columnList = "taskType"),
        @Index(name = "idx_task_def_status", columnList = "status")
})
public class TaskDefinition implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Unique task type identifier
     */
    @Column(name = "task_type", nullable = false, unique = true, length = 64)
    private String taskType;

    /**
     * Human-readable task name
     */
    @Column(name = "task_name", nullable = false, length = 128)
    private String taskName;

    /**
     * Executor type to handle this task
     */
    @Column(name = "executor_type", nullable = false, length = 64)
    private String executorType;

    /**
     * Executor-specific configuration (JSON)
     */
    @Type(JsonType.class)
    @Column(name = "executor_config", columnDefinition = "jsonb")
    private Map<String, Object> executorConfig;

    /**
     * Retry policy configuration (JSON)
     * Example: {"maxAttempts": 3, "initialDelay": 1000, "multiplier": 2.0, "maxDelay": 60000}
     */
    @Type(JsonType.class)
    @Column(name = "retry_policy", columnDefinition = "jsonb")
    private Map<String, Object> retryPolicy;

    /**
     * Task execution timeout in seconds
     */
    @Column(name = "timeout_seconds")
    @Builder.Default
    private Integer timeoutSeconds = 60;

    /**
     * Rate limit (requests per second)
     */
    @Column(name = "rate_limit")
    @Builder.Default
    private Integer rateLimit = 100;

    /**
     * Task description
     */
    @Column(name = "description", columnDefinition = "text")
    private String description;

    /**
     * Task definition status: ENABLED / DISABLED
     */
    @Column(name = "status", length = 16)
    @Builder.Default
    private String status = "ENABLED";

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        this.createdAt = now;
        this.updatedAt = now;

        // Set default retry policy if not specified
        if (this.retryPolicy == null) {
            this.retryPolicy = Map.of(
                    "maxAttempts", 3,
                    "initialDelay", 1000,
                    "multiplier", 2.0,
                    "maxDelay", 60000
            );
        }
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * Check if task definition is enabled
     */
    public boolean isEnabled() {
        return "ENABLED".equalsIgnoreCase(this.status);
    }

    /**
     * Enable task definition
     */
    public void enable() {
        this.status = "ENABLED";
    }

    /**
     * Disable task definition
     */
    public void disable() {
        this.status = "DISABLED";
    }

    /**
     * Get max retry attempts from retry policy
     */
    public int getMaxRetryAttempts() {
        if (retryPolicy == null || !retryPolicy.containsKey("maxAttempts")) {
            return 3;
        }
        Object value = retryPolicy.get("maxAttempts");
        return value instanceof Number ? ((Number) value).intValue() : 3;
    }

    /**
     * Get initial retry delay from retry policy
     */
    public long getInitialRetryDelay() {
        if (retryPolicy == null || !retryPolicy.containsKey("initialDelay")) {
            return 1000L;
        }
        Object value = retryPolicy.get("initialDelay");
        return value instanceof Number ? ((Number) value).longValue() : 1000L;
    }

    /**
     * Get retry multiplier from retry policy
     */
    public double getRetryMultiplier() {
        if (retryPolicy == null || !retryPolicy.containsKey("multiplier")) {
            return 2.0;
        }
        Object value = retryPolicy.get("multiplier");
        return value instanceof Number ? ((Number) value).doubleValue() : 2.0;
    }

    /**
     * Get max retry delay from retry policy
     */
    public long getMaxRetryDelay() {
        if (retryPolicy == null || !retryPolicy.containsKey("maxDelay")) {
            return 60000L;
        }
        Object value = retryPolicy.get("maxDelay");
        return value instanceof Number ? ((Number) value).longValue() : 60000L;
    }
}
