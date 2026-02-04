package com.lite.task.infrastructure.redis;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for DeduplicationChecker
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("DeduplicationChecker Tests")
class DeduplicationCheckerTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    private DeduplicationChecker deduplicationChecker;

    @BeforeEach
    void setUp() {
        deduplicationChecker = new DeduplicationChecker(redisTemplate);
    }

    @Nested
    @DisplayName("isDuplicate Tests")
    class IsDuplicateTests {

        @Test
        @DisplayName("Should return true when task is duplicate")
        void shouldReturnTrueWhenTaskIsDuplicate() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.hasKey(anyString())).thenReturn(true);

            // When
            boolean isDuplicate = deduplicationChecker.isDuplicate("HTTP_CALLBACK", params);

            // Then
            assertTrue(isDuplicate);
            verify(redisTemplate).hasKey(contains("task:dedup:HTTP_CALLBACK:"));
        }

        @Test
        @DisplayName("Should return false when task is not duplicate")
        void shouldReturnFalseWhenTaskIsNotDuplicate() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.hasKey(anyString())).thenReturn(false);

            // When
            boolean isDuplicate = deduplicationChecker.isDuplicate("HTTP_CALLBACK", params);

            // Then
            assertFalse(isDuplicate);
        }

        @Test
        @DisplayName("Should generate consistent hash for same parameters")
        void shouldGenerateConsistentHashForSameParameters() {
            // Given
            Map<String, Object> params1 = new LinkedHashMap<>();
            params1.put("a", "1");
            params1.put("b", "2");

            Map<String, Object> params2 = new LinkedHashMap<>();
            params2.put("b", "2");
            params2.put("a", "1");

            when(redisTemplate.hasKey(anyString())).thenReturn(false);

            // When
            deduplicationChecker.isDuplicate("HTTP_CALLBACK", params1);
            deduplicationChecker.isDuplicate("HTTP_CALLBACK", params2);

            // Then - both should check the same key (parameters are sorted)
            verify(redisTemplate, times(2)).hasKey(anyString());
        }

        @Test
        @DisplayName("Should check duplicate by custom key")
        void shouldCheckDuplicateByCustomKey() {
            // Given
            when(redisTemplate.hasKey(anyString())).thenReturn(true);

            // When
            boolean isDuplicate = deduplicationChecker.isDuplicate("HTTP_CALLBACK", "custom-unique-key");

            // Then
            assertTrue(isDuplicate);
            verify(redisTemplate).hasKey(contains("task:dedup:HTTP_CALLBACK:"));
        }
    }

    @Nested
    @DisplayName("markAsProcessed Tests")
    class MarkAsProcessedTests {

        @Test
        @DisplayName("Should mark task as processed with TTL")
        void shouldMarkTaskAsProcessedWithTTL() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.opsForValue()).thenReturn(valueOperations);

            // When
            deduplicationChecker.markAsProcessed("HTTP_CALLBACK", params, Duration.ofHours(1));

            // Then
            verify(valueOperations).set(
                    contains("task:dedup:HTTP_CALLBACK:"),
                    eq("1"),
                    eq(Duration.ofHours(1))
            );
        }

        @Test
        @DisplayName("Should mark task as processed with default TTL")
        void shouldMarkTaskAsProcessedWithDefaultTTL() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.opsForValue()).thenReturn(valueOperations);

            // When
            deduplicationChecker.markAsProcessed("HTTP_CALLBACK", params);

            // Then
            verify(valueOperations).set(
                    contains("task:dedup:HTTP_CALLBACK:"),
                    eq("1"),
                    eq(Duration.ofHours(24))
            );
        }

        @Test
        @DisplayName("Should mark task as processed by custom key")
        void shouldMarkTaskAsProcessedByCustomKey() {
            // Given
            when(redisTemplate.opsForValue()).thenReturn(valueOperations);

            // When
            deduplicationChecker.markAsProcessed("HTTP_CALLBACK", "custom-key", Duration.ofMinutes(30));

            // Then
            verify(valueOperations).set(
                    contains("task:dedup:HTTP_CALLBACK:"),
                    eq("1"),
                    eq(Duration.ofMinutes(30))
            );
        }
    }

    @Nested
    @DisplayName("checkAndMark Tests")
    class CheckAndMarkTests {

        @Test
        @DisplayName("Should return true and mark when task is new")
        void shouldReturnTrueAndMarkWhenTaskIsNew() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                    .thenReturn(true);

            // When
            boolean isNew = deduplicationChecker.checkAndMark("HTTP_CALLBACK", params, Duration.ofHours(1));

            // Then
            assertTrue(isNew);
            verify(valueOperations).setIfAbsent(
                    contains("task:dedup:HTTP_CALLBACK:"),
                    eq("1"),
                    eq(Duration.ofHours(1))
            );
        }

        @Test
        @DisplayName("Should return false when task is duplicate")
        void shouldReturnFalseWhenTaskIsDuplicate() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                    .thenReturn(false);

            // When
            boolean isNew = deduplicationChecker.checkAndMark("HTTP_CALLBACK", params, Duration.ofHours(1));

            // Then
            assertFalse(isNew);
        }

        @Test
        @DisplayName("Should use default TTL when not specified")
        void shouldUseDefaultTTLWhenNotSpecified() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                    .thenReturn(true);

            // When
            deduplicationChecker.checkAndMark("HTTP_CALLBACK", params);

            // Then
            verify(valueOperations).setIfAbsent(
                    anyString(),
                    eq("1"),
                    eq(Duration.ofHours(24))
            );
        }

        @Test
        @DisplayName("Should check and mark by task ID")
        void shouldCheckAndMarkByTaskId() {
            // Given
            when(redisTemplate.opsForValue()).thenReturn(valueOperations);
            when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                    .thenReturn(true);

            // When
            boolean isNew = deduplicationChecker.checkAndMarkByTaskId(
                    "HTTP_CALLBACK", "task-123", Duration.ofHours(1));

            // Then
            assertTrue(isNew);
            verify(valueOperations).setIfAbsent(
                    eq("task:dedup:HTTP_CALLBACK:task-123"),
                    eq("1"),
                    eq(Duration.ofHours(1))
            );
        }
    }

    @Nested
    @DisplayName("remove Tests")
    class RemoveTests {

        @Test
        @DisplayName("Should remove deduplication mark")
        void shouldRemoveDeduplicationMark() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            // When
            deduplicationChecker.remove("HTTP_CALLBACK", params);

            // Then
            verify(redisTemplate).delete(contains("task:dedup:HTTP_CALLBACK:"));
        }

        @Test
        @DisplayName("Should remove by task ID")
        void shouldRemoveByTaskId() {
            // When
            deduplicationChecker.removeByTaskId("HTTP_CALLBACK", "task-123");

            // Then
            verify(redisTemplate).delete("task:dedup:HTTP_CALLBACK:task-123");
        }
    }

    @Nested
    @DisplayName("getTtl Tests")
    class GetTtlTests {

        @Test
        @DisplayName("Should return TTL in seconds")
        void shouldReturnTTLInSeconds() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS)))
                    .thenReturn(3600L);

            // When
            long ttl = deduplicationChecker.getTtl("HTTP_CALLBACK", params);

            // Then
            assertEquals(3600L, ttl);
        }

        @Test
        @DisplayName("Should return -2 when key does not exist")
        void shouldReturnMinusTwoWhenKeyNotExists() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS)))
                    .thenReturn(null);

            // When
            long ttl = deduplicationChecker.getTtl("HTTP_CALLBACK", params);

            // Then
            assertEquals(-2L, ttl);
        }

        @Test
        @DisplayName("Should return -1 when no expiry")
        void shouldReturnMinusOneWhenNoExpiry() {
            // Given
            Map<String, Object> params = new HashMap<>();
            params.put("url", "https://example.com");

            when(redisTemplate.getExpire(anyString(), eq(TimeUnit.SECONDS)))
                    .thenReturn(-1L);

            // When
            long ttl = deduplicationChecker.getTtl("HTTP_CALLBACK", params);

            // Then
            assertEquals(-1L, ttl);
        }
    }

    @Nested
    @DisplayName("Hash Generation Tests")
    class HashGenerationTests {

        @Test
        @DisplayName("Should handle empty parameters")
        void shouldHandleEmptyParameters() {
            // Given
            Map<String, Object> params = new HashMap<>();
            when(redisTemplate.hasKey(anyString())).thenReturn(false);

            // When
            boolean isDuplicate = deduplicationChecker.isDuplicate("HTTP_CALLBACK", params);

            // Then
            assertFalse(isDuplicate);
            verify(redisTemplate).hasKey(contains("task:dedup:HTTP_CALLBACK:"));
        }

        @Test
        @DisplayName("Should handle null parameters")
        void shouldHandleNullParameters() {
            // Given
            when(redisTemplate.hasKey(anyString())).thenReturn(false);

            // When
            boolean isDuplicate = deduplicationChecker.isDuplicate("HTTP_CALLBACK", (Map<String, Object>) null);

            // Then
            assertFalse(isDuplicate);
            verify(redisTemplate).hasKey(contains("task:dedup:HTTP_CALLBACK:"));
        }

        @Test
        @DisplayName("Should generate different hashes for different parameters")
        void shouldGenerateDifferentHashesForDifferentParameters() {
            // Given
            Map<String, Object> params1 = new HashMap<>();
            params1.put("url", "https://example1.com");

            Map<String, Object> params2 = new HashMap<>();
            params2.put("url", "https://example2.com");

            when(redisTemplate.hasKey(anyString())).thenReturn(false);

            // When
            deduplicationChecker.isDuplicate("HTTP_CALLBACK", params1);
            deduplicationChecker.isDuplicate("HTTP_CALLBACK", params2);

            // Then - should call hasKey with different keys
            verify(redisTemplate, times(2)).hasKey(argThat(key ->
                    key.startsWith("task:dedup:HTTP_CALLBACK:")));
        }
    }
}
