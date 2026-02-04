package com.lite.task.infrastructure.redis;

import com.lite.task.common.enums.TaskPriority;
import com.lite.task.common.enums.TaskStatus;
import com.lite.task.domain.task.entity.TaskInstance;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for TaskCacheOperator
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("TaskCacheOperator Tests")
class TaskCacheOperatorTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    private TaskCacheOperator taskCacheOperator;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        taskCacheOperator = new TaskCacheOperator(redisTemplate);
    }

    private TaskInstance createTestTask(String taskId, TaskStatus status) {
        return TaskInstance.builder()
                .taskId(taskId)
                .taskDefId(1L)
                .taskType("TEST_TASK")
                .status(status)
                .priority(TaskPriority.DEFAULT.getLevel())
                .retryCount(0)
                .maxRetry(3)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();
    }

    @Nested
    @DisplayName("Save Tests")
    class SaveTests {

        @Test
        @DisplayName("Should save task to Redis hash")
        void shouldSaveTaskToRedisHash() {
            TaskInstance task = createTestTask("task-001", TaskStatus.PENDING);

            taskCacheOperator.save(task);

            verify(hashOperations).putAll(eq("task:data:task-001"), anyMap());
        }

        @Test
        @DisplayName("Should set TTL for terminal task")
        void shouldSetTtlForTerminalTask() {
            TaskInstance task = createTestTask("task-002", TaskStatus.SUCCESS);

            taskCacheOperator.save(task);

            verify(hashOperations).putAll(eq("task:data:task-002"), anyMap());
            verify(redisTemplate).expire("task:data:task-002", 24, TimeUnit.HOURS);
        }

        @Test
        @DisplayName("Should not set TTL for non-terminal task")
        void shouldNotSetTtlForNonTerminalTask() {
            TaskInstance task = createTestTask("task-003", TaskStatus.RUNNING);

            taskCacheOperator.save(task);

            verify(hashOperations).putAll(eq("task:data:task-003"), anyMap());
            verify(redisTemplate, never()).expire(anyString(), anyLong(), any());
        }

        @Test
        @DisplayName("Should save all task fields correctly")
        @SuppressWarnings("unchecked")
        void shouldSaveAllTaskFieldsCorrectly() {
            TaskInstance task = TaskInstance.builder()
                    .taskId("task-004")
                    .taskDefId(100L)
                    .taskType("HTTP_CALLBACK")
                    .status(TaskStatus.PENDING)
                    .priority(1)
                    .params(Map.of("url", "http://example.com"))
                    .retryCount(2)
                    .maxRetry(5)
                    .callbackUrl("http://callback.url")
                    .createdBy("user123")
                    .executorId("executor-001")
                    .createdAt(LocalDateTime.of(2024, 1, 1, 10, 0, 0))
                    .updatedAt(LocalDateTime.of(2024, 1, 1, 10, 30, 0))
                    .build();

            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.save(task);

            verify(hashOperations).putAll(eq("task:data:task-004"), captor.capture());

            Map<String, String> savedHash = captor.getValue();
            assertEquals("task-004", savedHash.get("taskId"));
            assertEquals("100", savedHash.get("taskDefId"));
            assertEquals("HTTP_CALLBACK", savedHash.get("taskType"));
            assertEquals("PENDING", savedHash.get("status"));
            assertEquals("1", savedHash.get("priority"));
            assertEquals("2", savedHash.get("retryCount"));
            assertEquals("5", savedHash.get("maxRetry"));
            assertEquals("http://callback.url", savedHash.get("callbackUrl"));
            assertEquals("user123", savedHash.get("createdBy"));
            assertEquals("executor-001", savedHash.get("executorId"));
        }
    }

    @Nested
    @DisplayName("Get Tests")
    class GetTests {

        @Test
        @DisplayName("Should return task when exists")
        void shouldReturnTaskWhenExists() {
            Map<Object, Object> hash = new HashMap<>();
            hash.put("taskId", "task-001");
            hash.put("taskDefId", "1");
            hash.put("taskType", "TEST_TASK");
            hash.put("status", "PENDING");
            hash.put("priority", "2");
            hash.put("retryCount", "0");
            hash.put("maxRetry", "3");
            hash.put("createdAt", "2024-01-01T10:00:00");
            hash.put("updatedAt", "2024-01-01T10:00:00");

            when(hashOperations.entries("task:data:task-001")).thenReturn(hash);

            TaskInstance result = taskCacheOperator.get("task-001");

            assertNotNull(result);
            assertEquals("task-001", result.getTaskId());
            assertEquals("TEST_TASK", result.getTaskType());
            assertEquals(TaskStatus.PENDING, result.getStatus());
        }

        @Test
        @DisplayName("Should return null when task not found")
        void shouldReturnNullWhenTaskNotFound() {
            when(hashOperations.entries("task:data:unknown")).thenReturn(new HashMap<>());

            TaskInstance result = taskCacheOperator.get("unknown");

            assertNull(result);
        }

        @Test
        @DisplayName("Should parse params as JSON map")
        void shouldParseParamsAsJsonMap() {
            Map<Object, Object> hash = new HashMap<>();
            hash.put("taskId", "task-002");
            hash.put("taskType", "TEST");
            hash.put("status", "PENDING");
            hash.put("priority", "2");
            hash.put("retryCount", "0");
            hash.put("maxRetry", "3");
            hash.put("params", "{\"key\":\"value\",\"number\":123}");

            when(hashOperations.entries("task:data:task-002")).thenReturn(hash);

            TaskInstance result = taskCacheOperator.get("task-002");

            assertNotNull(result.getParams());
            assertEquals("value", result.getParams().get("key"));
            assertEquals(123, result.getParams().get("number"));
        }

        @Test
        @DisplayName("Should handle null result field (not stored in Redis)")
        void shouldHandleNullResultField() {
            Map<Object, Object> hash = new HashMap<>();
            hash.put("taskId", "task-003");
            hash.put("taskType", "TEST");
            hash.put("status", "SUCCESS");
            hash.put("priority", "2");
            hash.put("retryCount", "0");
            hash.put("maxRetry", "3");

            when(hashOperations.entries("task:data:task-003")).thenReturn(hash);

            TaskInstance result = taskCacheOperator.get("task-003");

            assertNull(result.getResult(), "Result should be null (not stored in Redis)");
        }
    }

    @Nested
    @DisplayName("Exists Tests")
    class ExistsTests {

        @Test
        @DisplayName("Should return true when task exists")
        void shouldReturnTrueWhenTaskExists() {
            when(redisTemplate.hasKey("task:data:task-001")).thenReturn(true);

            assertTrue(taskCacheOperator.exists("task-001"));
        }

        @Test
        @DisplayName("Should return false when task not exists")
        void shouldReturnFalseWhenTaskNotExists() {
            when(redisTemplate.hasKey("task:data:unknown")).thenReturn(false);

            assertFalse(taskCacheOperator.exists("unknown"));
        }
    }

    @Nested
    @DisplayName("Delete Tests")
    class DeleteTests {

        @Test
        @DisplayName("Should delete task from cache")
        void shouldDeleteTaskFromCache() {
            when(redisTemplate.delete("task:data:task-001")).thenReturn(true);

            assertTrue(taskCacheOperator.delete("task-001"));
            verify(redisTemplate).delete("task:data:task-001");
        }

        @Test
        @DisplayName("Should return false when delete fails")
        void shouldReturnFalseWhenDeleteFails() {
            when(redisTemplate.delete("task:data:unknown")).thenReturn(false);

            assertFalse(taskCacheOperator.delete("unknown"));
        }
    }

    @Nested
    @DisplayName("UpdateStatus Tests")
    class UpdateStatusTests {

        @Test
        @DisplayName("Should update status")
        @SuppressWarnings("unchecked")
        void shouldUpdateStatus() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateStatus("task-001", TaskStatus.RUNNING);

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            assertEquals("RUNNING", captor.getValue().get("status"));
            assertNotNull(captor.getValue().get("updatedAt"));
        }

        @Test
        @DisplayName("Should set TTL when status is terminal")
        void shouldSetTtlWhenStatusIsTerminal() {
            taskCacheOperator.updateStatus("task-001", TaskStatus.SUCCESS);

            verify(redisTemplate).expire("task:data:task-001", 24, TimeUnit.HOURS);
        }

        @Test
        @DisplayName("Should not set TTL when status is non-terminal")
        void shouldNotSetTtlWhenStatusIsNonTerminal() {
            taskCacheOperator.updateStatus("task-001", TaskStatus.RUNNING);

            verify(redisTemplate, never()).expire(anyString(), anyLong(), any());
        }
    }

    @Nested
    @DisplayName("UpdateOnStart Tests")
    class UpdateOnStartTests {

        @Test
        @DisplayName("Should update task on start execution")
        @SuppressWarnings("unchecked")
        void shouldUpdateTaskOnStartExecution() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateOnStart("task-001", "executor-123");

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            Map<String, String> updates = captor.getValue();

            assertEquals("RUNNING", updates.get("status"));
            assertEquals("executor-123", updates.get("executorId"));
            assertNotNull(updates.get("startedAt"));
            assertNotNull(updates.get("updatedAt"));
        }

        @Test
        @DisplayName("Should handle null executor ID")
        @SuppressWarnings("unchecked")
        void shouldHandleNullExecutorId() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateOnStart("task-001", null);

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            assertEquals("", captor.getValue().get("executorId"));
        }
    }

    @Nested
    @DisplayName("UpdateOnComplete Tests")
    class UpdateOnCompleteTests {

        @Test
        @DisplayName("Should update task on completion")
        @SuppressWarnings("unchecked")
        void shouldUpdateTaskOnCompletion() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateOnComplete("task-001");

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            Map<String, String> updates = captor.getValue();

            assertEquals("SUCCESS", updates.get("status"));
            assertNotNull(updates.get("finishedAt"));
            assertNotNull(updates.get("updatedAt"));
        }

        @Test
        @DisplayName("Should set TTL on completion")
        void shouldSetTtlOnCompletion() {
            taskCacheOperator.updateOnComplete("task-001");

            verify(redisTemplate).expire("task:data:task-001", 24, TimeUnit.HOURS);
        }
    }

    @Nested
    @DisplayName("UpdateOnFailure Tests")
    class UpdateOnFailureTests {

        @Test
        @DisplayName("Should update task on failure")
        @SuppressWarnings("unchecked")
        void shouldUpdateTaskOnFailure() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateOnFailure("task-001", "Connection timeout", TaskStatus.FAILED);

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            Map<String, String> updates = captor.getValue();

            assertEquals("FAILED", updates.get("status"));
            assertEquals("Connection timeout", updates.get("errorMessage"));
            assertNotNull(updates.get("finishedAt"));
            assertNotNull(updates.get("updatedAt"));
        }

        @Test
        @DisplayName("Should set TTL when failure is terminal")
        void shouldSetTtlWhenFailureIsTerminal() {
            taskCacheOperator.updateOnFailure("task-001", "Error", TaskStatus.DEAD);

            verify(redisTemplate).expire("task:data:task-001", 24, TimeUnit.HOURS);
        }

        @Test
        @DisplayName("Should not set TTL for retrying status")
        void shouldNotSetTtlForRetryingStatus() {
            taskCacheOperator.updateOnFailure("task-001", "Error", TaskStatus.RETRYING);

            verify(redisTemplate, never()).expire(anyString(), anyLong(), any());
        }

        @Test
        @DisplayName("Should handle null error message")
        @SuppressWarnings("unchecked")
        void shouldHandleNullErrorMessage() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateOnFailure("task-001", null, TaskStatus.FAILED);

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            assertEquals("", captor.getValue().get("errorMessage"));
        }
    }

    @Nested
    @DisplayName("UpdateRetry Tests")
    class UpdateRetryTests {

        @Test
        @DisplayName("Should update task for retry")
        @SuppressWarnings("unchecked")
        void shouldUpdateTaskForRetry() {
            ArgumentCaptor<Map<String, String>> captor = ArgumentCaptor.forClass(Map.class);

            taskCacheOperator.updateRetry("task-001", 2, TaskStatus.RETRYING);

            verify(hashOperations).putAll(eq("task:data:task-001"), captor.capture());
            Map<String, String> updates = captor.getValue();

            assertEquals("RETRYING", updates.get("status"));
            assertEquals("2", updates.get("retryCount"));
            assertEquals("", updates.get("startedAt"));
            assertEquals("", updates.get("finishedAt"));
            assertEquals("", updates.get("executorId"));
            assertEquals("", updates.get("errorMessage"));
        }
    }

    @Nested
    @DisplayName("Field Access Tests")
    class FieldAccessTests {

        @Test
        @DisplayName("Should get single field value")
        void shouldGetSingleFieldValue() {
            when(hashOperations.get("task:data:task-001", "taskType")).thenReturn("HTTP_CALLBACK");

            String result = taskCacheOperator.getField("task-001", "taskType");

            assertEquals("HTTP_CALLBACK", result);
        }

        @Test
        @DisplayName("Should return null for missing field")
        void shouldReturnNullForMissingField() {
            when(hashOperations.get("task:data:task-001", "unknown")).thenReturn(null);

            String result = taskCacheOperator.getField("task-001", "unknown");

            assertNull(result);
        }

        @Test
        @DisplayName("Should get task status")
        void shouldGetTaskStatus() {
            when(hashOperations.get("task:data:task-001", "status")).thenReturn("RUNNING");

            TaskStatus result = taskCacheOperator.getStatus("task-001");

            assertEquals(TaskStatus.RUNNING, result);
        }

        @Test
        @DisplayName("Should return null for invalid status")
        void shouldReturnNullForInvalidStatus() {
            when(hashOperations.get("task:data:task-001", "status")).thenReturn("INVALID");

            TaskStatus result = taskCacheOperator.getStatus("task-001");

            assertNull(result);
        }

        @Test
        @DisplayName("Should get task type")
        void shouldGetTaskType() {
            when(hashOperations.get("task:data:task-001", "taskType")).thenReturn("EMAIL_SEND");

            String result = taskCacheOperator.getTaskType("task-001");

            assertEquals("EMAIL_SEND", result);
        }

        @Test
        @DisplayName("Should get retry count")
        void shouldGetRetryCount() {
            when(hashOperations.get("task:data:task-001", "retryCount")).thenReturn("3");

            int result = taskCacheOperator.getRetryCount("task-001");

            assertEquals(3, result);
        }

        @Test
        @DisplayName("Should return zero for missing retry count")
        void shouldReturnZeroForMissingRetryCount() {
            when(hashOperations.get("task:data:task-001", "retryCount")).thenReturn(null);

            int result = taskCacheOperator.getRetryCount("task-001");

            assertEquals(0, result);
        }
    }

    @Nested
    @DisplayName("Key Generation Tests")
    class KeyGenerationTests {

        @Test
        @DisplayName("Should generate correct key format")
        void shouldGenerateCorrectKeyFormat() {
            taskCacheOperator.exists("my-task-123");

            verify(redisTemplate).hasKey("task:data:my-task-123");
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    class EdgeCasesTests {

        @Test
        @DisplayName("Should handle empty hash from Redis")
        void shouldHandleEmptyHashFromRedis() {
            when(hashOperations.entries("task:data:task-001")).thenReturn(new HashMap<>());

            TaskInstance result = taskCacheOperator.get("task-001");

            assertNull(result);
        }

        @Test
        @DisplayName("Should handle null hash from Redis")
        void shouldHandleNullHashFromRedis() {
            when(hashOperations.entries("task:data:task-001")).thenReturn(null);

            TaskInstance result = taskCacheOperator.get("task-001");

            assertNull(result);
        }

        @Test
        @DisplayName("Should handle task with all null optional fields")
        void shouldHandleTaskWithAllNullOptionalFields() {
            TaskInstance task = TaskInstance.builder()
                    .taskId("task-001")
                    .taskType("TEST")
                    .status(TaskStatus.PENDING)
                    .build();

            assertDoesNotThrow(() -> taskCacheOperator.save(task));
            verify(hashOperations).putAll(eq("task:data:task-001"), anyMap());
        }

        @Test
        @DisplayName("Should handle malformed date in hash")
        void shouldHandleMalformedDateInHash() {
            Map<Object, Object> hash = new HashMap<>();
            hash.put("taskId", "task-001");
            hash.put("taskType", "TEST");
            hash.put("status", "PENDING");
            hash.put("priority", "2");
            hash.put("retryCount", "0");
            hash.put("maxRetry", "3");
            hash.put("createdAt", "not-a-date");

            when(hashOperations.entries("task:data:task-001")).thenReturn(hash);

            TaskInstance result = taskCacheOperator.get("task-001");

            assertNotNull(result);
            assertNull(result.getCreatedAt());
        }

        @Test
        @DisplayName("Should handle malformed integer in hash")
        void shouldHandleMalformedIntegerInHash() {
            Map<Object, Object> hash = new HashMap<>();
            hash.put("taskId", "task-001");
            hash.put("taskType", "TEST");
            hash.put("status", "PENDING");
            hash.put("priority", "not-a-number");
            hash.put("retryCount", "abc");
            hash.put("maxRetry", "xyz");

            when(hashOperations.entries("task:data:task-001")).thenReturn(hash);

            TaskInstance result = taskCacheOperator.get("task-001");

            assertNotNull(result);
            assertEquals(TaskPriority.DEFAULT.getLevel(), result.getPriority());
            assertEquals(0, result.getRetryCount());
            assertEquals(3, result.getMaxRetry());
        }
    }
}
