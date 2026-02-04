package com.lite.task.common.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Snowflake ID Generator
 *
 * ID structure (64 bits):
 * - 1 bit: sign bit (always 0)
 * - 41 bits: timestamp (milliseconds since epoch, ~69 years)
 * - 10 bits: worker ID (1024 workers)
 * - 12 bits: sequence (4096 per ms per worker)
 *
 * @author lite-task-dispatcher
 */
public class IdGenerator {

    /**
     * Start epoch: 2024-01-01 00:00:00 UTC
     */
    private static final long EPOCH = 1704067200000L;

    /**
     * Bits allocation
     */
    private static final long WORKER_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    /**
     * Max values
     */
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS);
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);

    /**
     * Shift amounts
     */
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    /**
     * Singleton instance
     */
    private static final IdGenerator INSTANCE = new IdGenerator();

    private final long workerId;
    private long sequence = 0L;
    private long lastTimestamp = -1L;

    private IdGenerator() {
        this.workerId = generateWorkerId();
    }

    private IdGenerator(long workerId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException(
                    String.format("Worker ID must be between 0 and %d", MAX_WORKER_ID));
        }
        this.workerId = workerId;
    }

    /**
     * Generate unique ID
     */
    public synchronized long nextId() {
        long currentTimestamp = System.currentTimeMillis();

        // Clock moved backwards - handle gracefully
        if (currentTimestamp < lastTimestamp) {
            long offset = lastTimestamp - currentTimestamp;
            if (offset <= 5) {
                // Wait for clock to catch up
                try {
                    Thread.sleep(offset << 1);
                    currentTimestamp = System.currentTimeMillis();
                    if (currentTimestamp < lastTimestamp) {
                        throw new RuntimeException(
                                String.format("Clock moved backwards. Refusing to generate ID for %d ms", offset));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Thread interrupted while waiting for clock", e);
                }
            } else {
                throw new RuntimeException(
                        String.format("Clock moved backwards. Refusing to generate ID for %d ms", offset));
            }
        }

        if (currentTimestamp == lastTimestamp) {
            // Same millisecond - increment sequence
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                // Sequence exhausted - wait for next millisecond
                currentTimestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // New millisecond - reset sequence with random start
            sequence = ThreadLocalRandom.current().nextLong(0, 3);
        }

        lastTimestamp = currentTimestamp;

        return ((currentTimestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;
    }

    /**
     * Generate ID as string
     */
    public String nextIdStr() {
        return String.valueOf(nextId());
    }

    /**
     * Wait until next millisecond
     */
    private long waitNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }

    /**
     * Generate worker ID from MAC address and process ID
     */
    private static long generateWorkerId() {
        try {
            StringBuilder sb = new StringBuilder();

            // Get MAC address
            InetAddress ip = InetAddress.getLocalHost();
            NetworkInterface network = NetworkInterface.getByInetAddress(ip);
            if (network != null) {
                byte[] mac = network.getHardwareAddress();
                if (mac != null) {
                    for (byte b : mac) {
                        sb.append(String.format("%02X", b));
                    }
                }
            }

            // Add process ID
            sb.append(ProcessHandle.current().pid());

            // Hash and truncate to fit worker ID bits
            return Math.abs(sb.toString().hashCode()) % (MAX_WORKER_ID + 1);
        } catch (Exception e) {
            // Fallback to random worker ID
            return ThreadLocalRandom.current().nextLong(0, MAX_WORKER_ID + 1);
        }
    }

    /**
     * Parse timestamp from ID
     */
    public static long parseTimestamp(long id) {
        return (id >> TIMESTAMP_SHIFT) + EPOCH;
    }

    /**
     * Parse worker ID from ID
     */
    public static long parseWorkerId(long id) {
        return (id >> WORKER_ID_SHIFT) & MAX_WORKER_ID;
    }

    /**
     * Parse sequence from ID
     */
    public static long parseSequence(long id) {
        return id & MAX_SEQUENCE;
    }

    /**
     * Get singleton instance
     */
    public static IdGenerator getInstance() {
        return INSTANCE;
    }

    /**
     * Generate ID using singleton
     */
    public static long generateId() {
        return INSTANCE.nextId();
    }

    /**
     * Generate ID string using singleton
     */
    public static String generateIdStr() {
        return INSTANCE.nextIdStr();
    }
}
