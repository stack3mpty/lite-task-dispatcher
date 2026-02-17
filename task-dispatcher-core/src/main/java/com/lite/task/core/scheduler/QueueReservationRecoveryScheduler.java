package com.lite.task.core.scheduler;

import com.lite.task.infrastructure.redis.DistributedLock;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Recovers expired queue reservations.
 *
 * If a worker reserves a task and crashes before ACK, task is moved back to ready queue.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class QueueReservationRecoveryScheduler {

    private final TaskQueueOperator taskQueueOperator;
    private final DistributedLock distributedLock;
    private final MeterRegistry meterRegistry;

    @Value("${task.scheduler.queue-recovery.enabled:true}")
    private boolean enabled;

    @Value("${task.scheduler.queue-recovery.batch-size:200}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${task.scheduler.queue-recovery.interval-ms:1000}")
    public void recoverExpiredReservations() {
        if (!enabled) {
            return;
        }

        String lockKey = "task:queue:reservation:recovery";
        if (!distributedLock.tryLock(lockKey, 1, 10)) {
            return;
        }

        try {
            long reclaimed = taskQueueOperator.reclaimExpiredReservations(batchSize);
            if (reclaimed > 0) {
                meterRegistry.counter("task.queue.reclaim.total").increment(reclaimed);
                log.warn("Reclaimed expired queue reservations: count={}", reclaimed);
            }
        } catch (Exception e) {
            log.error("Failed to recover expired queue reservations: {}", e.getMessage(), e);
        } finally {
            distributedLock.unlock(lockKey);
        }
    }
}

