package com.lite.task.infrastructure.redis;

import com.lite.task.common.exception.RateLimitException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RateLimiter
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("RateLimiter Tests")
class RateLimiterTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private HashOperations<String, Object, Object> hashOperations;

    private RateLimiter rateLimiter;

    @BeforeEach
    void setUp() {
        rateLimiter = new RateLimiter(redisTemplate);
    }

    @SuppressWarnings("unchecked")
    private void mockExecuteReturn(Long result) {
        when(redisTemplate.execute(
                any(RedisScript.class),
                anyList(),
                any(String.class),
                any(String.class),
                any(String.class),
                any(String.class)
        )).thenReturn(result);
    }

    @Nested
    @DisplayName("tryAcquire Tests")
    class TryAcquireTests {

        @Test
        @DisplayName("Should return true when permit acquired")
        void shouldReturnTrueWhenPermitAcquired() {
            // Given
            mockExecuteReturn(1L);

            // When
            boolean result = rateLimiter.tryAcquire("HTTP_CALLBACK", 100, 10);

            // Then
            assertTrue(result);
        }

        @Test
        @DisplayName("Should return false when rate limit exceeded")
        void shouldReturnFalseWhenRateLimitExceeded() {
            // Given
            mockExecuteReturn(0L);

            // When
            boolean result = rateLimiter.tryAcquire("HTTP_CALLBACK", 100, 10);

            // Then
            assertFalse(result);
        }

        @Test
        @DisplayName("Should return false when result is null")
        void shouldReturnFalseWhenResultIsNull() {
            // Given
            mockExecuteReturn(null);

            // When
            boolean result = rateLimiter.tryAcquire("HTTP_CALLBACK", 100, 10);

            // Then
            assertFalse(result);
        }

        @Test
        @DisplayName("Should use correct key pattern")
        @SuppressWarnings("unchecked")
        void shouldUseCorrectKeyPattern() {
            // Given
            mockExecuteReturn(1L);

            // When
            rateLimiter.tryAcquire("MY_TASK_TYPE", 100, 10);

            // Then
            verify(redisTemplate).execute(
                    any(RedisScript.class),
                    argThat((List<String> keys) ->
                            keys.size() == 1 && keys.get(0).equals("task:rate:limit:MY_TASK_TYPE")),
                    any(String.class),
                    any(String.class),
                    any(String.class),
                    any(String.class)
            );
        }

        @Test
        @DisplayName("Should acquire multiple permits")
        void shouldAcquireMultiplePermits() {
            // Given
            mockExecuteReturn(1L);

            // When
            boolean result = rateLimiter.tryAcquire("HTTP_CALLBACK", 100, 10, 5);

            // Then
            assertTrue(result);
        }
    }

    @Nested
    @DisplayName("acquire Tests")
    class AcquireTests {

        @Test
        @DisplayName("Should not throw when permit acquired")
        void shouldNotThrowWhenPermitAcquired() {
            // Given
            mockExecuteReturn(1L);

            // When & Then
            assertDoesNotThrow(() -> rateLimiter.acquire("HTTP_CALLBACK", 100, 10));
        }

        @Test
        @DisplayName("Should throw RateLimitException when rate limit exceeded")
        void shouldThrowRateLimitExceptionWhenExceeded() {
            // Given
            mockExecuteReturn(0L);

            // When & Then
            RateLimitException exception = assertThrows(RateLimitException.class,
                    () -> rateLimiter.acquire("HTTP_CALLBACK", 100, 10));

            assertTrue(exception.getMessage().contains("HTTP_CALLBACK"));
        }
    }

    @Nested
    @DisplayName("getCurrentTokens Tests")
    class GetCurrentTokensTests {

        @Test
        @DisplayName("Should return current token count")
        void shouldReturnCurrentTokenCount() {
            // Given
            when(redisTemplate.opsForHash()).thenReturn(hashOperations);
            when(hashOperations.get("task:rate:limit:HTTP_CALLBACK", "tokens"))
                    .thenReturn("50");

            // When
            long tokens = rateLimiter.getCurrentTokens("HTTP_CALLBACK");

            // Then
            assertEquals(50L, tokens);
        }

        @Test
        @DisplayName("Should return -1 when key does not exist")
        void shouldReturnMinusOneWhenKeyNotExists() {
            // Given
            when(redisTemplate.opsForHash()).thenReturn(hashOperations);
            when(hashOperations.get("task:rate:limit:HTTP_CALLBACK", "tokens"))
                    .thenReturn(null);

            // When
            long tokens = rateLimiter.getCurrentTokens("HTTP_CALLBACK");

            // Then
            assertEquals(-1L, tokens);
        }
    }

    @Nested
    @DisplayName("reset Tests")
    class ResetTests {

        @Test
        @DisplayName("Should delete rate limit key")
        void shouldDeleteRateLimitKey() {
            // When
            rateLimiter.reset("HTTP_CALLBACK");

            // Then
            verify(redisTemplate).delete("task:rate:limit:HTTP_CALLBACK");
        }
    }

    @Nested
    @DisplayName("exists Tests")
    class ExistsTests {

        @Test
        @DisplayName("Should return true when key exists")
        void shouldReturnTrueWhenKeyExists() {
            // Given
            when(redisTemplate.hasKey("task:rate:limit:HTTP_CALLBACK"))
                    .thenReturn(true);

            // When
            boolean exists = rateLimiter.exists("HTTP_CALLBACK");

            // Then
            assertTrue(exists);
        }

        @Test
        @DisplayName("Should return false when key does not exist")
        void shouldReturnFalseWhenKeyNotExists() {
            // Given
            when(redisTemplate.hasKey("task:rate:limit:HTTP_CALLBACK"))
                    .thenReturn(false);

            // When
            boolean exists = rateLimiter.exists("HTTP_CALLBACK");

            // Then
            assertFalse(exists);
        }

        @Test
        @DisplayName("Should return false when hasKey returns null")
        void shouldReturnFalseWhenHasKeyReturnsNull() {
            // Given
            when(redisTemplate.hasKey("task:rate:limit:HTTP_CALLBACK"))
                    .thenReturn(null);

            // When
            boolean exists = rateLimiter.exists("HTTP_CALLBACK");

            // Then
            assertFalse(exists);
        }
    }

    @Nested
    @DisplayName("Key Pattern Tests")
    class KeyPatternTests {

        @Test
        @DisplayName("Should handle task types with special characters")
        @SuppressWarnings("unchecked")
        void shouldHandleTaskTypesWithSpecialCharacters() {
            // Given
            mockExecuteReturn(1L);

            // When
            rateLimiter.tryAcquire("TASK-TYPE_123", 100, 10);

            // Then
            verify(redisTemplate).execute(
                    any(RedisScript.class),
                    argThat((List<String> keys) ->
                            keys.get(0).equals("task:rate:limit:TASK-TYPE_123")),
                    any(String.class),
                    any(String.class),
                    any(String.class),
                    any(String.class)
            );
        }
    }
}
