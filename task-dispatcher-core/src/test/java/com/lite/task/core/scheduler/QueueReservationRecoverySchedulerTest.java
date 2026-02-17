package com.lite.task.core.scheduler;

import com.lite.task.infrastructure.redis.DistributedLock;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("QueueReservationRecoveryScheduler Tests")
class QueueReservationRecoverySchedulerTest {

    @Mock
    private TaskQueueOperator taskQueueOperator;
    @Mock
    private DistributedLock distributedLock;
    @Mock
    private MeterRegistry meterRegistry;
    @Mock
    private Counter counter;

    private QueueReservationRecoveryScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new QueueReservationRecoveryScheduler(taskQueueOperator, distributedLock, meterRegistry);
        ReflectionTestUtils.setField(scheduler, "enabled", true);
        ReflectionTestUtils.setField(scheduler, "batchSize", 200);
    }

    @Nested
    @DisplayName("Recovery Flow")
    class RecoveryFlow {

        @Test
        @DisplayName("Should skip when scheduler is disabled")
        void shouldSkipWhenDisabled() {
            ReflectionTestUtils.setField(scheduler, "enabled", false);

            scheduler.recoverExpiredReservations();

            verifyNoInteractions(distributedLock, taskQueueOperator, meterRegistry);
        }

        @Test
        @DisplayName("Should skip when lock is not acquired")
        void shouldSkipWhenLockNotAcquired() {
            when(distributedLock.tryLock("task:queue:reservation:recovery", 1, 10)).thenReturn(false);

            scheduler.recoverExpiredReservations();

            verify(distributedLock).tryLock("task:queue:reservation:recovery", 1, 10);
            verifyNoInteractions(taskQueueOperator, meterRegistry);
        }

        @Test
        @DisplayName("Should reclaim and record metric")
        void shouldReclaimAndRecordMetric() {
            when(distributedLock.tryLock("task:queue:reservation:recovery", 1, 10)).thenReturn(true);
            when(taskQueueOperator.reclaimExpiredReservations(200)).thenReturn(3L);
            when(meterRegistry.counter("task.queue.reclaim.total")).thenReturn(counter);

            scheduler.recoverExpiredReservations();

            verify(taskQueueOperator).reclaimExpiredReservations(200);
            verify(meterRegistry).counter("task.queue.reclaim.total");
            verify(counter).increment(3.0);
            verify(distributedLock).unlock("task:queue:reservation:recovery");
        }
    }
}

