package com.lite.task.infrastructure.metrics;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Registers business metrics for queue backlog and running state.
 */
@Component
@RequiredArgsConstructor
public class TaskMetricsCollector {

    private final MeterRegistry meterRegistry;
    private final TaskQueueOperator taskQueueOperator;
    private final DelayQueueOperator delayQueueOperator;
    private final TaskInstanceRepository taskInstanceRepository;

    @PostConstruct
    public void registerGauges() {
        for (TaskPriority priority : TaskPriority.values()) {
            Gauge.builder("task.queue.length", taskQueueOperator, operator -> operator.size(priority))
                    .description("Queue length by priority")
                    .tag("priority", priority.name())
                    .register(meterRegistry);

            Gauge.builder("task.queue.processing.length", taskQueueOperator, operator -> operator.processingSize(priority))
                    .description("Processing queue length by priority")
                    .tag("priority", priority.name())
                    .register(meterRegistry);
        }

        Gauge.builder("task.queue.total.length", taskQueueOperator, TaskQueueOperator::totalSize)
                .description("Total queue backlog across all priorities")
                .register(meterRegistry);

        Gauge.builder("task.queue.processing.total.length", taskQueueOperator, TaskQueueOperator::totalProcessingSize)
                .description("Total processing queue size across all priorities")
                .register(meterRegistry);

        Gauge.builder("task.delay.queue.length", delayQueueOperator, DelayQueueOperator::size)
                .description("Delay queue backlog")
                .register(meterRegistry);

        Gauge.builder("task.running.count", taskInstanceRepository,
                        repository -> repository.countByStatus(TaskStatus.RUNNING))
                .description("Count of running tasks in DB")
                .register(meterRegistry);
    }
}
