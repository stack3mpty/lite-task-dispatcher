package com.lite.task.api.dto.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * Dashboard Statistics Response DTO
 *
 * @author lite-task-dispatcher
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DashboardStatsResponse {

    /**
     * Total number of tasks
     */
    private long totalTasks;

    /**
     * Number of pending tasks
     */
    private long pendingTasks;

    /**
     * Number of running tasks
     */
    private long runningTasks;

    /**
     * Number of successful tasks
     */
    private long successTasks;

    /**
     * Number of failed tasks
     */
    private long failedTasks;

    /**
     * Queue lengths by priority (P0, P1, P2, P3, P4)
     */
    private Map<String, Long> queueLengths;
}
