package com.lite.task.core.scheduler;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DelayTaskScheduler
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DelayTaskScheduler Tests")
class DelayTaskSchedulerTest {

    @Mock
    private DelayQueueOperator delayQueueOperator;

    @Mock
    private TaskQueueOperator taskQueueOperator;

    @Mock
    private TaskCacheOperator taskCacheOperator;

    private DelayTaskScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new DelayTaskScheduler(delayQueueOperator, taskQueueOperator, taskCacheOperator);
        ReflectionTestUtils.setField(scheduler, "batchSize", 100);
        ReflectionTestUtils.setField(scheduler, "enabled", true);
    }

    private TaskInstance createTestTask(String taskId, int priority) {
        return TaskInstance.builder()
                .taskId(taskId)
                .taskType("TEST_TASK")
                .status(TaskStatus.PENDING)
                .priority(priority)
                .build();
    }

    @Nested
    @DisplayName("Poll and Transfer Tests")
    class PollAndTransferTests {

        @Test
        @DisplayName("Should transfer ready tasks to execution queue")
        void shouldTransferReadyTasksToExecutionQueue() {
            // Given
            List<String> readyTaskIds = Arrays.asList("task-001", "task-002");
            when(delayQueueOperator.pollReady(100)).thenReturn(readyTaskIds);
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 2));
            when(taskCacheOperator.get("task-002")).thenReturn(createTestTask("task-002", 1));

            // When
            scheduler.pollAndTransfer();

            // Then
            verify(delayQueueOperator).pollReady(100);
            verify(taskQueueOperator).push("task-001", TaskPriority.P2);
            verify(taskQueueOperator).push("task-002", TaskPriority.P1);
        }

        @Test
        @DisplayName("Should do nothing when no ready tasks")
        void shouldDoNothingWhenNoReadyTasks() {
            // Given
            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.emptyList());

            // When
            scheduler.pollAndTransfer();

            // Then
            verify(delayQueueOperator).pollReady(100);
            verifyNoInteractions(taskQueueOperator);
            verifyNoInteractions(taskCacheOperator);
        }

        @Test
        @DisplayName("Should skip task when not found in cache")
        void shouldSkipTaskWhenNotFoundInCache() {
            // Given
            List<String> readyTaskIds = Arrays.asList("task-001", "task-002");
            when(delayQueueOperator.pollReady(100)).thenReturn(readyTaskIds);
            when(taskCacheOperator.get("task-001")).thenReturn(null);
            when(taskCacheOperator.get("task-002")).thenReturn(createTestTask("task-002", 2));

            // When
            scheduler.pollAndTransfer();

            // Then
            verify(taskQueueOperator, never()).push(eq("task-001"), any());
            verify(taskQueueOperator).push("task-002", TaskPriority.P2);
        }

        @Test
        @DisplayName("Should skip task when getPriorityEnum throws exception")
        void shouldSkipTaskWhenPriorityIsNull() {
            // Given - task with null priority will cause NPE in getPriorityEnum()
            TaskInstance task = TaskInstance.builder()
                    .taskId("task-001")
                    .taskType("TEST")
                    .status(TaskStatus.PENDING)
                    .priority(null)
                    .build();

            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.singletonList("task-001"));
            when(taskCacheOperator.get("task-001")).thenReturn(task);

            // When
            scheduler.pollAndTransfer();

            // Then - task should be skipped due to exception
            verify(taskQueueOperator, never()).push(anyString(), any());
        }

        @Test
        @DisplayName("Should not run when disabled")
        void shouldNotRunWhenDisabled() {
            // Given
            ReflectionTestUtils.setField(scheduler, "enabled", false);

            // When
            scheduler.pollAndTransfer();

            // Then
            verifyNoInteractions(delayQueueOperator);
            verifyNoInteractions(taskQueueOperator);
            verifyNoInteractions(taskCacheOperator);
        }

        @Test
        @DisplayName("Should handle exception gracefully")
        void shouldHandleExceptionGracefully() {
            // Given
            when(delayQueueOperator.pollReady(100)).thenThrow(new RuntimeException("Redis error"));

            // When & Then - should not throw
            assertDoesNotThrow(() -> scheduler.pollAndTransfer());
        }

        @Test
        @DisplayName("Should continue processing when one task fails")
        void shouldContinueProcessingWhenOneTaskFails() {
            // Given
            List<String> readyTaskIds = Arrays.asList("task-001", "task-002", "task-003");
            when(delayQueueOperator.pollReady(100)).thenReturn(readyTaskIds);
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 2));
            when(taskCacheOperator.get("task-002")).thenThrow(new RuntimeException("Cache error"));
            when(taskCacheOperator.get("task-003")).thenReturn(createTestTask("task-003", 2));

            // When
            scheduler.pollAndTransfer();

            // Then
            verify(taskQueueOperator).push("task-001", TaskPriority.P2);
            verify(taskQueueOperator).push("task-003", TaskPriority.DEFAULT);
        }
    }

    @Nested
    @DisplayName("Priority Mapping Tests")
    class PriorityMappingTests {

        @Test
        @DisplayName("Should map priority 0 to CRITICAL")
        void shouldMapPriority0ToCritical() {
            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.singletonList("task-001"));
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 0));

            scheduler.pollAndTransfer();

            verify(taskQueueOperator).push("task-001", TaskPriority.P0);
        }

        @Test
        @DisplayName("Should map priority 1 to HIGH")
        void shouldMapPriority1ToHigh() {
            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.singletonList("task-001"));
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 1));

            scheduler.pollAndTransfer();

            verify(taskQueueOperator).push("task-001", TaskPriority.P1);
        }

        @Test
        @DisplayName("Should map priority 2 to DEFAULT")
        void shouldMapPriority2ToDefault() {
            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.singletonList("task-001"));
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 2));

            scheduler.pollAndTransfer();

            verify(taskQueueOperator).push("task-001", TaskPriority.P2);
        }

        @Test
        @DisplayName("Should map priority 3 to LOW")
        void shouldMapPriority3ToLow() {
            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.singletonList("task-001"));
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 3));

            scheduler.pollAndTransfer();

            verify(taskQueueOperator).push("task-001", TaskPriority.P3);
        }

        @Test
        @DisplayName("Should map priority 4 to LOWEST")
        void shouldMapPriority4ToLowest() {
            when(delayQueueOperator.pollReady(100)).thenReturn(Collections.singletonList("task-001"));
            when(taskCacheOperator.get("task-001")).thenReturn(createTestTask("task-001", 4));

            scheduler.pollAndTransfer();

            verify(taskQueueOperator).push("task-001", TaskPriority.P4);
        }
    }

    @Nested
    @DisplayName("Queue Size Tests")
    class QueueSizeTests {

        @Test
        @DisplayName("Should return delay queue size")
        void shouldReturnDelayQueueSize() {
            when(delayQueueOperator.size()).thenReturn(42L);

            long size = scheduler.getDelayQueueSize();

            assertEquals(42L, size);
        }

        @Test
        @DisplayName("Should return ready count")
        void shouldReturnReadyCount() {
            when(delayQueueOperator.readyCount()).thenReturn(10L);

            long count = scheduler.getReadyCount();

            assertEquals(10L, count);
        }
    }

    @Nested
    @DisplayName("Batch Size Tests")
    class BatchSizeTests {

        @Test
        @DisplayName("Should use configured batch size")
        void shouldUseConfiguredBatchSize() {
            ReflectionTestUtils.setField(scheduler, "batchSize", 50);
            when(delayQueueOperator.pollReady(50)).thenReturn(Collections.emptyList());

            scheduler.pollAndTransfer();

            verify(delayQueueOperator).pollReady(50);
        }
    }
}
