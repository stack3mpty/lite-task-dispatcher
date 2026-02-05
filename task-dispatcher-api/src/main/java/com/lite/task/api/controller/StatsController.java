package com.lite.task.api.controller;

import com.lite.task.api.dto.response.DashboardStatsResponse;
import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.model.Result;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Statistics Controller
 *
 * REST API for dashboard and statistics
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/stats")
@RequiredArgsConstructor
@Tag(name = "Stats API", description = "Dashboard and statistics operations")
public class StatsController {

    private final TaskInstanceRepository taskInstanceRepository;
    private final TaskQueueOperator taskQueueOperator;

    /**
     * Get dashboard statistics
     */
    @GetMapping("/dashboard")
    @Operation(summary = "Get dashboard statistics")
    public Result<DashboardStatsResponse> getDashboardStats() {
        log.debug("Getting dashboard statistics");

        // Count tasks by status
        long pendingTasks = taskInstanceRepository.countByStatus(TaskStatus.PENDING);
        long runningTasks = taskInstanceRepository.countByStatus(TaskStatus.RUNNING);
        long successTasks = taskInstanceRepository.countByStatus(TaskStatus.SUCCESS);
        long failedTasks = taskInstanceRepository.countByStatus(TaskStatus.FAILED)
                + taskInstanceRepository.countByStatus(TaskStatus.DEAD);

        long totalTasks = taskInstanceRepository.count();

        // Get queue lengths from Redis
        Map<String, Long> queueLengths = new LinkedHashMap<>();
        for (TaskPriority priority : TaskPriority.values()) {
            queueLengths.put(priority.name(), taskQueueOperator.size(priority));
        }

        DashboardStatsResponse stats = DashboardStatsResponse.builder()
                .totalTasks(totalTasks)
                .pendingTasks(pendingTasks)
                .runningTasks(runningTasks)
                .successTasks(successTasks)
                .failedTasks(failedTasks)
                .queueLengths(queueLengths)
                .build();

        return Result.success(stats);
    }
}
