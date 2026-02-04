package com.lite.task.core.executor.impl;

import com.lite.task.core.executor.AbstractTaskExecutor;
import com.lite.task.core.executor.TaskResult;
import com.lite.task.domain.task.valueobject.TaskContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * Log Executor
 *
 * A simple executor that logs task execution for testing and debugging.
 * Can be used to verify the complete execution pipeline.
 *
 * Parameters:
 * - message: Custom message to log (optional)
 * - delay: Simulated execution delay in ms (optional, default: 0)
 * - fail: If true, simulates failure (optional, default: false)
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
public class LogExecutor extends AbstractTaskExecutor {

    private static final String TYPE = "LOG";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    protected TaskResult doExecute(TaskContext context) {
        String message = context.getParam("message", "Task executed");
        int delay = context.getParam("delay", 0);
        boolean shouldFail = context.getParam("fail", false);

        log.info("=== LogExecutor Start ===");
        log.info("TaskId: {}", context.getTaskId());
        log.info("TaskType: {}", context.getTaskType());
        log.info("AttemptNumber: {}/{}", context.getAttemptNumber(), context.getMaxAttempts());
        log.info("Params: {}", context.getParams());
        log.info("Message: {}", message);

        // Simulate processing time
        if (delay > 0) {
            log.info("Simulating delay: {}ms", delay);
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Task interrupted", e);
            }
        }

        // Simulate failure
        if (shouldFail) {
            log.warn("Simulating failure as requested");
            throw new RuntimeException("Simulated failure: " + message);
        }

        log.info("=== LogExecutor End ===");

        Map<String, Object> resultData = new HashMap<>();
        resultData.put("message", message);
        resultData.put("taskId", context.getTaskId());
        resultData.put("executedAt", System.currentTimeMillis());

        return TaskResult.success("Log task executed successfully", resultData);
    }
}
