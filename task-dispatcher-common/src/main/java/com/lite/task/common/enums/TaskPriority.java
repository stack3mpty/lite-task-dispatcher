package com.lite.task.common.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Task Priority Enum
 *
 * Lower number means higher priority (0 is highest)
 *
 * @author lite-task-dispatcher
 */
@Getter
@AllArgsConstructor
public enum TaskPriority {

    /**
     * Highest priority - Critical tasks
     */
    P0(0, "Critical"),

    /**
     * High priority - Urgent tasks
     */
    P1(1, "High"),

    /**
     * Normal priority - Regular tasks
     */
    P2(2, "Normal"),

    /**
     * Low priority - Background tasks
     */
    P3(3, "Low"),

    /**
     * Lowest priority - Batch tasks
     */
    P4(4, "Lowest");

    private final int level;
    private final String description;

    /**
     * Default priority
     */
    public static final TaskPriority DEFAULT = P2;

    /**
     * Get queue name suffix for this priority
     */
    public String getQueueSuffix() {
        return "p" + level;
    }

    public static TaskPriority fromLevel(int level) {
        for (TaskPriority priority : values()) {
            if (priority.getLevel() == level) {
                return priority;
            }
        }
        // Default to P2 if invalid level
        return DEFAULT;
    }

    /**
     * Check if this priority is higher than another
     */
    public boolean isHigherThan(TaskPriority other) {
        return this.level < other.level;
    }
}
