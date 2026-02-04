package com.lite.task.domain.task.valueobject;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * Task Context Value Object
 *
 * Contains all information needed to execute a task
 *
 * @author lite-task-dispatcher
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TaskContext implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Task ID
     */
    private String taskId;

    /**
     * Task type
     */
    private String taskType;

    /**
     * Executor type
     */
    private String executorType;

    /**
     * Task input parameters
     */
    private Map<String, Object> params;

    /**
     * Executor-specific configuration
     */
    private Map<String, Object> executorConfig;

    /**
     * Current attempt number (1-based)
     */
    private int attemptNumber;

    /**
     * Maximum attempts allowed
     */
    private int maxAttempts;

    /**
     * Timeout in seconds
     */
    private int timeoutSeconds;

    /**
     * Callback URL (optional)
     */
    private String callbackUrl;

    /**
     * Trace ID for distributed tracing
     */
    private String traceId;

    /**
     * Additional metadata
     */
    private Map<String, Object> metadata;

    /**
     * Builder for TaskContext
     */
    public static TaskContextBuilder builder() {
        return new TaskContextBuilder();
    }

    public static class TaskContextBuilder {
        private String taskId;
        private String taskType;
        private String executorType;
        private Map<String, Object> params;
        private Map<String, Object> executorConfig;
        private int attemptNumber = 1;
        private int maxAttempts = 3;
        private int timeoutSeconds = 60;
        private String callbackUrl;
        private String traceId;
        private Map<String, Object> metadata;

        public TaskContextBuilder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public TaskContextBuilder taskType(String taskType) {
            this.taskType = taskType;
            return this;
        }

        public TaskContextBuilder executorType(String executorType) {
            this.executorType = executorType;
            return this;
        }

        public TaskContextBuilder params(Map<String, Object> params) {
            this.params = params;
            return this;
        }

        public TaskContextBuilder executorConfig(Map<String, Object> executorConfig) {
            this.executorConfig = executorConfig;
            return this;
        }

        public TaskContextBuilder attemptNumber(int attemptNumber) {
            this.attemptNumber = attemptNumber;
            return this;
        }

        public TaskContextBuilder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public TaskContextBuilder timeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
            return this;
        }

        public TaskContextBuilder callbackUrl(String callbackUrl) {
            this.callbackUrl = callbackUrl;
            return this;
        }

        public TaskContextBuilder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public TaskContextBuilder metadata(Map<String, Object> metadata) {
            this.metadata = metadata;
            return this;
        }

        public TaskContext build() {
            return new TaskContext(taskId, taskType, executorType, params, executorConfig,
                    attemptNumber, maxAttempts, timeoutSeconds, callbackUrl, traceId, metadata);
        }
    }

    /**
     * Get parameter value
     */
    @SuppressWarnings("unchecked")
    public <T> T getParam(String key) {
        if (params == null) {
            return null;
        }
        return (T) params.get(key);
    }

    /**
     * Get parameter value with default
     */
    @SuppressWarnings("unchecked")
    public <T> T getParam(String key, T defaultValue) {
        if (params == null || !params.containsKey(key)) {
            return defaultValue;
        }
        return (T) params.get(key);
    }

    /**
     * Get config value
     */
    @SuppressWarnings("unchecked")
    public <T> T getConfig(String key) {
        if (executorConfig == null) {
            return null;
        }
        return (T) executorConfig.get(key);
    }

    /**
     * Get config value with default
     */
    @SuppressWarnings("unchecked")
    public <T> T getConfig(String key, T defaultValue) {
        if (executorConfig == null || !executorConfig.containsKey(key)) {
            return defaultValue;
        }
        return (T) executorConfig.get(key);
    }

    /**
     * Check if this is a retry attempt
     */
    public boolean isRetry() {
        return attemptNumber > 1;
    }

    /**
     * Check if this is the last attempt
     */
    public boolean isLastAttempt() {
        return attemptNumber >= maxAttempts;
    }
}
