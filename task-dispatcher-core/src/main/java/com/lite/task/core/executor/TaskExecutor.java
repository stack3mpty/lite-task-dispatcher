package com.lite.task.core.executor;

import com.lite.task.domain.task.valueobject.TaskContext;

/**
 * Task Executor Interface (SPI)
 *
 * Strategy pattern for task execution
 * Implementations should be registered via @Component or SPI
 *
 * @author lite-task-dispatcher
 */
public interface TaskExecutor {

    /**
     * Get executor type identifier
     *
     * @return Executor type (e.g., "HTTP_CALLBACK", "EMAIL", "DATA_SYNC")
     */
    String getType();

    /**
     * Execute task
     *
     * @param context Task execution context
     * @return Execution result
     */
    TaskResult execute(TaskContext context);

    /**
     * Check if this executor supports the given task
     *
     * @param context Task context
     * @return true if this executor can handle the task
     */
    default boolean supports(TaskContext context) {
        return getType().equalsIgnoreCase(context.getExecutorType());
    }

    /**
     * Get executor priority (for ordering when multiple executors match)
     * Lower value = higher priority
     *
     * @return Priority value
     */
    default int getPriority() {
        return 100;
    }
}
