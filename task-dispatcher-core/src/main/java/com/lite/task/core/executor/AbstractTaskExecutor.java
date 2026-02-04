package com.lite.task.core.executor;

import com.lite.task.domain.task.valueobject.TaskContext;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract Task Executor (Template Method Pattern)
 *
 * Provides common execution flow:
 * 1. Pre-execute validation and setup
 * 2. Execute task (implemented by subclass)
 * 3. Post-execute cleanup and logging
 * 4. Exception handling
 *
 * @author lite-task-dispatcher
 */
@Slf4j
public abstract class AbstractTaskExecutor implements TaskExecutor {

    @Override
    public final TaskResult execute(TaskContext context) {
        long startTime = System.currentTimeMillis();
        String taskId = context.getTaskId();

        log.info("Starting task execution: taskId={}, type={}, executor={}, attempt={}/{}",
                taskId, context.getTaskType(), getType(),
                context.getAttemptNumber(), context.getMaxAttempts());

        try {
            // 1. Pre-execute hook
            preExecute(context);

            // 2. Validate context
            validate(context);

            // 3. Execute task (subclass implementation)
            TaskResult result = doExecute(context);

            // 4. Post-execute hook
            postExecute(context, result);

            long duration = System.currentTimeMillis() - startTime;
            result.setDurationMs(duration);

            log.info("Task execution completed: taskId={}, success={}, duration={}ms",
                    taskId, result.isSuccess(), duration);

            return result;

        } catch (Exception e) {
            long duration = System.currentTimeMillis() - startTime;

            log.error("Task execution failed: taskId={}, error={}, duration={}ms",
                    taskId, e.getMessage(), duration, e);

            // 5. Handle exception
            TaskResult result = handleException(context, e);
            result.setDurationMs(duration);
            return result;
        }
    }

    /**
     * Execute task - must be implemented by subclass
     *
     * @param context Task context
     * @return Execution result
     */
    protected abstract TaskResult doExecute(TaskContext context);

    /**
     * Pre-execute hook - called before task execution
     * Override to add custom pre-processing
     *
     * @param context Task context
     */
    protected void preExecute(TaskContext context) {
        // Default: no-op
    }

    /**
     * Validate task context
     * Override to add custom validation
     *
     * @param context Task context
     */
    protected void validate(TaskContext context) {
        if (context == null) {
            throw new IllegalArgumentException("Task context cannot be null");
        }
        if (context.getTaskId() == null || context.getTaskId().isEmpty()) {
            throw new IllegalArgumentException("Task ID cannot be null or empty");
        }
    }

    /**
     * Post-execute hook - called after successful execution
     * Override to add custom post-processing
     *
     * @param context Task context
     * @param result  Execution result
     */
    protected void postExecute(TaskContext context, TaskResult result) {
        // Default: no-op
    }

    /**
     * Handle execution exception
     * Override to customize exception handling
     *
     * @param context   Task context
     * @param exception Exception that occurred
     * @return TaskResult representing the failure
     */
    protected TaskResult handleException(TaskContext context, Exception exception) {
        boolean retryable = isRetryableException(exception);

        return TaskResult.builder()
                .success(false)
                .message(exception.getMessage())
                .errorDetail(getStackTrace(exception))
                .retryable(retryable)
                .build();
    }

    /**
     * Check if exception is retryable
     * Override to customize retry logic
     *
     * @param exception Exception to check
     * @return true if task should be retried
     */
    protected boolean isRetryableException(Exception exception) {
        // By default, most exceptions are retryable
        // Non-retryable: IllegalArgumentException, NullPointerException, etc.
        if (exception instanceof IllegalArgumentException ||
                exception instanceof NullPointerException ||
                exception instanceof IllegalStateException) {
            return false;
        }
        return true;
    }

    /**
     * Get stack trace as string
     */
    private String getStackTrace(Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append(e.getClass().getName()).append(": ").append(e.getMessage()).append("\n");

        StackTraceElement[] stackTrace = e.getStackTrace();
        int limit = Math.min(stackTrace.length, 10); // Limit to 10 frames
        for (int i = 0; i < limit; i++) {
            sb.append("\tat ").append(stackTrace[i]).append("\n");
        }

        if (stackTrace.length > limit) {
            sb.append("\t... ").append(stackTrace.length - limit).append(" more\n");
        }

        if (e.getCause() != null) {
            sb.append("Caused by: ").append(e.getCause().getClass().getName())
                    .append(": ").append(e.getCause().getMessage());
        }

        return sb.toString();
    }
}
