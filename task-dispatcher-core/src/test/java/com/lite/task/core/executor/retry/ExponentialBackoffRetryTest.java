package com.lite.task.core.executor.retry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for ExponentialBackoffRetry
 */
@DisplayName("ExponentialBackoffRetry Tests")
class ExponentialBackoffRetryTest {

    @Nested
    @DisplayName("Builder Tests")
    class BuilderTests {

        @Test
        @DisplayName("Should create default policy with builder defaults")
        void shouldCreateDefaultPolicy() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder().build();

            assertEquals(3, policy.getMaxAttempts());
            assertEquals(1000L, policy.getInitialDelay());
            assertEquals(60000L, policy.getMaxDelay());
            assertEquals(2.0, policy.getMultiplier());
            assertEquals(0.2, policy.getJitterFactor());
        }

        @Test
        @DisplayName("Should create policy with custom values")
        void shouldCreatePolicyWithCustomValues() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(5)
                    .initialDelay(500L)
                    .maxDelay(30000L)
                    .multiplier(3.0)
                    .jitterFactor(0.1)
                    .build();

            assertEquals(5, policy.getMaxAttempts());
            assertEquals(500L, policy.getInitialDelay());
            assertEquals(30000L, policy.getMaxDelay());
            assertEquals(3.0, policy.getMultiplier());
            assertEquals(0.1, policy.getJitterFactor());
        }

        @Test
        @DisplayName("Should create default policy using static method")
        void shouldCreateDefaultPolicyUsingStaticMethod() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.defaultPolicy();

            assertEquals(3, policy.getMaxAttempts());
        }

        @Test
        @DisplayName("Should create policy with custom max attempts")
        void shouldCreatePolicyWithCustomMaxAttempts() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.withMaxAttempts(10);

            assertEquals(10, policy.getMaxAttempts());
        }

        @Test
        @DisplayName("Should create policy from config")
        void shouldCreatePolicyFromConfig() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.fromConfig(
                    5, 2000L, 1.5, 30000L);

            assertEquals(5, policy.getMaxAttempts());
            assertEquals(2000L, policy.getInitialDelay());
            assertEquals(1.5, policy.getMultiplier());
            assertEquals(30000L, policy.getMaxDelay());
        }
    }

    @Nested
    @DisplayName("shouldRetry Tests")
    class ShouldRetryTests {

        @Test
        @DisplayName("Should retry when attempts not exceeded")
        void shouldRetryWhenAttemptsNotExceeded() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .build();

            assertTrue(policy.shouldRetry(0, new RuntimeException()));
            assertTrue(policy.shouldRetry(1, new RuntimeException()));
            assertTrue(policy.shouldRetry(2, new RuntimeException()));
        }

        @Test
        @DisplayName("Should not retry when max attempts exceeded")
        void shouldNotRetryWhenMaxAttemptsExceeded() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .build();

            assertFalse(policy.shouldRetry(3, new RuntimeException()));
            assertFalse(policy.shouldRetry(4, new RuntimeException()));
        }

        @Test
        @DisplayName("Should not retry for non-retryable exception")
        void shouldNotRetryForNonRetryableException() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .build()
                    .addNonRetryable(IllegalArgumentException.class);

            assertFalse(policy.shouldRetry(1, new IllegalArgumentException("test")));
        }

        @Test
        @DisplayName("Should retry for retryable exception")
        void shouldRetryForRetryableException() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .build()
                    .addNonRetryable(IllegalArgumentException.class);

            assertTrue(policy.shouldRetry(1, new IOException("test")));
            assertTrue(policy.shouldRetry(1, new RuntimeException("test")));
        }

        @Test
        @DisplayName("Should handle subclass of non-retryable exception")
        void shouldHandleSubclassOfNonRetryableException() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .build()
                    .addNonRetryable(RuntimeException.class);

            // IllegalArgumentException is a subclass of RuntimeException
            assertFalse(policy.shouldRetry(1, new IllegalArgumentException("test")));
        }

        @Test
        @DisplayName("Should retry when exception is null")
        void shouldRetryWhenExceptionIsNull() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.defaultPolicy();

            assertTrue(policy.shouldRetry(1, null));
        }
    }

    @Nested
    @DisplayName("getNextDelay Tests")
    class GetNextDelayTests {

        @Test
        @DisplayName("Should calculate exponential delay")
        void shouldCalculateExponentialDelay() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .initialDelay(1000L)
                    .multiplier(2.0)
                    .maxDelay(60000L)
                    .jitterFactor(0.0) // Disable jitter for predictable testing
                    .build();

            assertEquals(1000L, policy.getNextDelay(1)); // 1000 * 2^0
            assertEquals(2000L, policy.getNextDelay(2)); // 1000 * 2^1
            assertEquals(4000L, policy.getNextDelay(3)); // 1000 * 2^2
            assertEquals(8000L, policy.getNextDelay(4)); // 1000 * 2^3
        }

        @Test
        @DisplayName("Should cap delay at maxDelay")
        void shouldCapDelayAtMaxDelay() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .initialDelay(1000L)
                    .multiplier(10.0)
                    .maxDelay(5000L)
                    .jitterFactor(0.0)
                    .build();

            assertEquals(1000L, policy.getNextDelay(1));  // 1000 * 10^0 = 1000
            assertEquals(5000L, policy.getNextDelay(2));  // 1000 * 10^1 = 10000 -> capped to 5000
            assertEquals(5000L, policy.getNextDelay(3));  // capped to 5000
        }

        @Test
        @DisplayName("Should add jitter when jitterFactor > 0")
        void shouldAddJitterWhenJitterFactorPositive() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .initialDelay(1000L)
                    .multiplier(2.0)
                    .maxDelay(60000L)
                    .jitterFactor(0.2)
                    .build();

            // With 20% jitter, delay should be between baseDelay and baseDelay * 1.2
            long baseDelay = 1000L;
            for (int i = 0; i < 100; i++) {
                long delay = policy.getNextDelay(1);
                assertTrue(delay >= baseDelay, "Delay should be >= base delay");
                assertTrue(delay <= baseDelay * 1.2, "Delay should be <= base delay * 1.2");
            }
        }

        @Test
        @DisplayName("Should have no jitter when jitterFactor is 0")
        void shouldHaveNoJitterWhenJitterFactorZero() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .initialDelay(1000L)
                    .multiplier(2.0)
                    .jitterFactor(0.0)
                    .build();

            // Without jitter, delay should be consistent
            long delay1 = policy.getNextDelay(1);
            long delay2 = policy.getNextDelay(1);
            assertEquals(delay1, delay2);
        }

        @Test
        @DisplayName("Should handle first attempt correctly")
        void shouldHandleFirstAttemptCorrectly() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .initialDelay(1000L)
                    .multiplier(2.0)
                    .jitterFactor(0.0)
                    .build();

            // First attempt (attempt=1) should use initialDelay
            assertEquals(1000L, policy.getNextDelay(1));
        }
    }

    @Nested
    @DisplayName("getAllDelays Tests")
    class GetAllDelaysTests {

        @Test
        @DisplayName("Should return all delays")
        void shouldReturnAllDelays() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(4)
                    .initialDelay(1000L)
                    .multiplier(2.0)
                    .jitterFactor(0.0)
                    .build();

            long[] delays = policy.getAllDelays();

            assertEquals(4, delays.length);
            assertEquals(1000L, delays[0]); // 1000 * 2^0
            assertEquals(2000L, delays[1]); // 1000 * 2^1
            assertEquals(4000L, delays[2]); // 1000 * 2^2
            assertEquals(8000L, delays[3]); // 1000 * 2^3
        }
    }

    @Nested
    @DisplayName("toString Tests")
    class ToStringTests {

        @Test
        @DisplayName("Should return formatted string")
        void shouldReturnFormattedString() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .initialDelay(1000L)
                    .maxDelay(60000L)
                    .multiplier(2.0)
                    .jitterFactor(0.2)
                    .build();

            String str = policy.toString();

            assertTrue(str.contains("maxAttempts=3"));
            assertTrue(str.contains("initialDelay=1000"));
            assertTrue(str.contains("maxDelay=60000"));
            assertTrue(str.contains("multiplier=2.0"));
            assertTrue(str.contains("jitterFactor=0.2"));
        }
    }

    @Nested
    @DisplayName("Integration Scenarios")
    class IntegrationScenarios {

        @Test
        @DisplayName("Should simulate typical retry scenario")
        void shouldSimulateTypicalRetryScenario() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(3)
                    .initialDelay(100L)
                    .multiplier(2.0)
                    .jitterFactor(0.0)
                    .build();

            int attempt = 0;
            Exception lastException = new IOException("Connection failed");

            // Simulate retry loop
            while (policy.shouldRetry(attempt, lastException)) {
                attempt++;
                long delay = policy.getNextDelay(attempt);

                // Verify delay increases exponentially
                if (attempt == 1) assertEquals(100L, delay);
                if (attempt == 2) assertEquals(200L, delay);
                if (attempt == 3) assertEquals(400L, delay);
            }

            assertEquals(3, attempt, "Should have made 3 attempts");
        }

        @Test
        @DisplayName("Should stop immediately for non-retryable exception")
        void shouldStopImmediatelyForNonRetryableException() {
            ExponentialBackoffRetry policy = ExponentialBackoffRetry.builder()
                    .maxAttempts(5)
                    .build()
                    .addNonRetryable(IllegalStateException.class);

            int attempt = 0;
            Exception nonRetryable = new IllegalStateException("Invalid state");

            while (policy.shouldRetry(attempt, nonRetryable)) {
                attempt++;
            }

            assertEquals(0, attempt, "Should not retry for non-retryable exception");
        }
    }
}
