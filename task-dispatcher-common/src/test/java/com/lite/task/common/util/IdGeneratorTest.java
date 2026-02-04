package com.lite.task.common.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for IdGenerator (Snowflake ID)
 */
@DisplayName("IdGenerator Tests")
class IdGeneratorTest {

    @Test
    @DisplayName("Should generate positive ID")
    void shouldGeneratePositiveId() {
        long id = IdGenerator.generateId();
        assertTrue(id > 0, "ID should be positive");
    }

    @Test
    @DisplayName("Should generate unique IDs")
    void shouldGenerateUniqueIds() {
        Set<Long> ids = new HashSet<>();
        int count = 10000;

        for (int i = 0; i < count; i++) {
            long id = IdGenerator.generateId();
            assertTrue(ids.add(id), "ID should be unique: " + id);
        }

        assertEquals(count, ids.size());
    }

    @Test
    @DisplayName("Should generate IDs in ascending order")
    void shouldGenerateIdsInAscendingOrder() {
        long previousId = 0;

        for (int i = 0; i < 1000; i++) {
            long id = IdGenerator.generateId();
            assertTrue(id > previousId, "ID should be ascending");
            previousId = id;
        }
    }

    @Test
    @DisplayName("Should generate unique IDs under concurrent access")
    void shouldGenerateUniqueIdsUnderConcurrency() throws InterruptedException {
        int threadCount = 10;
        int idsPerThread = 1000;
        Set<Long> allIds = ConcurrentHashMap.newKeySet();
        CountDownLatch latch = new CountDownLatch(threadCount);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < idsPerThread; j++) {
                        long id = IdGenerator.generateId();
                        allIds.add(id);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertEquals(threadCount * idsPerThread, allIds.size(),
                "All IDs should be unique across threads");
    }

    @Test
    @DisplayName("Should parse timestamp from ID correctly")
    void shouldParseTimestampCorrectly() {
        long beforeGenerate = System.currentTimeMillis();
        long id = IdGenerator.generateId();
        long afterGenerate = System.currentTimeMillis();

        long parsedTimestamp = IdGenerator.parseTimestamp(id);

        assertTrue(parsedTimestamp >= beforeGenerate,
                "Parsed timestamp should be >= time before generation");
        assertTrue(parsedTimestamp <= afterGenerate,
                "Parsed timestamp should be <= time after generation");
    }

    @Test
    @DisplayName("Should parse worker ID from ID")
    void shouldParseWorkerIdCorrectly() {
        long id = IdGenerator.generateId();
        long workerId = IdGenerator.parseWorkerId(id);

        // Worker ID should be in valid range (0-1023 for 10 bits)
        assertTrue(workerId >= 0, "Worker ID should be non-negative");
        assertTrue(workerId <= 1023, "Worker ID should be <= 1023");
    }

    @Test
    @DisplayName("Should parse sequence from ID")
    void shouldParseSequenceCorrectly() {
        long id = IdGenerator.generateId();
        long sequence = IdGenerator.parseSequence(id);

        // Sequence should be in valid range (0-4095 for 12 bits)
        assertTrue(sequence >= 0, "Sequence should be non-negative");
        assertTrue(sequence <= 4095, "Sequence should be <= 4095");
    }

    @Test
    @DisplayName("Should return same instance from getInstance")
    void shouldReturnSameInstance() {
        IdGenerator instance1 = IdGenerator.getInstance();
        IdGenerator instance2 = IdGenerator.getInstance();

        assertSame(instance1, instance2, "getInstance should return singleton");
    }

    @Test
    @DisplayName("Should generate ID string")
    void shouldGenerateIdString() {
        String idStr = IdGenerator.generateIdStr();

        assertNotNull(idStr);
        assertFalse(idStr.isEmpty());
        // Should be parseable as long
        assertDoesNotThrow(() -> Long.parseLong(idStr));
    }

    @RepeatedTest(5)
    @DisplayName("Should generate IDs with consistent worker ID")
    void shouldHaveConsistentWorkerId() {
        long id1 = IdGenerator.generateId();
        long id2 = IdGenerator.generateId();

        long workerId1 = IdGenerator.parseWorkerId(id1);
        long workerId2 = IdGenerator.parseWorkerId(id2);

        assertEquals(workerId1, workerId2,
                "Worker ID should be consistent within same instance");
    }

    @Test
    @DisplayName("ID structure should have 64 bits")
    void shouldHave64BitStructure() {
        long id = IdGenerator.generateId();

        // ID should fit in long (64 bits) and be positive (sign bit = 0)
        assertTrue(id > 0, "ID should be positive (sign bit = 0)");
        assertTrue(id < Long.MAX_VALUE, "ID should be less than Long.MAX_VALUE");
    }

    @Test
    @DisplayName("Should handle high throughput")
    void shouldHandleHighThroughput() {
        int count = 100000;
        Set<Long> ids = new HashSet<>();

        long startTime = System.currentTimeMillis();
        for (int i = 0; i < count; i++) {
            ids.add(IdGenerator.generateId());
        }
        long endTime = System.currentTimeMillis();

        assertEquals(count, ids.size(), "All IDs should be unique");

        long duration = endTime - startTime;
        double idsPerSecond = count / (duration / 1000.0);
        System.out.printf("Generated %d unique IDs in %d ms (%.0f IDs/second)%n",
                count, duration, idsPerSecond);
    }
}
