package com.lite.task.core.worker;

import com.lite.task.core.dispatcher.TaskDispatcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TaskWorker
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("TaskWorker Tests")
class TaskWorkerTest {

    @Mock
    private TaskDispatcher taskDispatcher;

    private TaskWorker taskWorker;

    @BeforeEach
    void setUp() {
        taskWorker = new TaskWorker(taskDispatcher);
        ReflectionTestUtils.setField(taskWorker, "enabled", true);
        ReflectionTestUtils.setField(taskWorker, "workerThreads", 1);
        ReflectionTestUtils.setField(taskWorker, "pollIntervalMs", 10L);
        ReflectionTestUtils.setField(taskWorker, "idleSleepMs", 50L);
    }

    @Nested
    @DisplayName("Lifecycle Tests")
    class LifecycleTests {

        @Test
        @DisplayName("Should start worker when enabled")
        void shouldStartWorkerWhenEnabled() throws Exception {
            // When
            taskWorker.start();

            // Then
            assertTrue(taskWorker.isRunning());

            // Cleanup
            taskWorker.stop();
        }

        @Test
        @DisplayName("Should not start worker when disabled")
        void shouldNotStartWorkerWhenDisabled() {
            // Given
            ReflectionTestUtils.setField(taskWorker, "enabled", false);

            // When
            taskWorker.start();

            // Then
            assertFalse(taskWorker.isRunning());
        }

        @Test
        @DisplayName("Should stop worker gracefully")
        void shouldStopWorkerGracefully() throws Exception {
            // Given
            taskWorker.start();
            assertTrue(taskWorker.isRunning());

            // When
            taskWorker.stop();

            // Then
            assertFalse(taskWorker.isRunning());
        }
    }

    @Nested
    @DisplayName("Execution Tests")
    class ExecutionTests {

        @Test
        @DisplayName("Should poll and execute tasks")
        void shouldPollAndExecuteTasks() throws Exception {
            // Given
            when(taskDispatcher.pollAndExecute())
                    .thenReturn(true)
                    .thenReturn(true)
                    .thenReturn(false);

            // When
            taskWorker.start();

            // Wait for some executions
            TimeUnit.MILLISECONDS.sleep(200);

            taskWorker.stop();

            // Then
            verify(taskDispatcher, atLeast(2)).pollAndExecute();
        }

        @Test
        @DisplayName("Should continue on exception")
        void shouldContinueOnException() throws Exception {
            // Given
            when(taskDispatcher.pollAndExecute())
                    .thenThrow(new RuntimeException("Test error"))
                    .thenReturn(true)
                    .thenReturn(false);

            // When
            taskWorker.start();

            // Wait for recovery
            TimeUnit.MILLISECONDS.sleep(200);

            taskWorker.stop();

            // Then - should have called multiple times despite exception
            verify(taskDispatcher, atLeast(2)).pollAndExecute();
        }

        @Test
        @DisplayName("Should sleep when no tasks available")
        void shouldSleepWhenNoTasksAvailable() throws Exception {
            // Given
            when(taskDispatcher.pollAndExecute()).thenReturn(false);

            // When
            taskWorker.start();

            // Short wait
            TimeUnit.MILLISECONDS.sleep(100);

            taskWorker.stop();

            // Then - should have called pollAndExecute but not too many times due to idle sleep
            verify(taskDispatcher, atMost(5)).pollAndExecute();
        }
    }

    @Nested
    @DisplayName("Configuration Tests")
    class ConfigurationTests {

        @Test
        @DisplayName("Should use configured thread count")
        void shouldUseConfiguredThreadCount() {
            // Given
            ReflectionTestUtils.setField(taskWorker, "workerThreads", 3);

            // Then
            assertEquals(3, taskWorker.getWorkerThreads());
        }
    }
}
