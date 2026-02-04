package com.lite.task.infrastructure.redis;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Distributed Lock
 *
 * Redisson-based distributed lock implementation
 *
 * Features:
 * - Automatic lock renewal (watchdog)
 * - Reentrant lock support
 * - Fair lock option
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DistributedLock {

    private final RedissonClient redissonClient;

    private static final String LOCK_KEY_PREFIX = "task:lock:";
    private static final long DEFAULT_WAIT_TIME = 3;
    private static final long DEFAULT_LEASE_TIME = 30;

    /**
     * Try to acquire lock
     *
     * @param lockKey   Lock key
     * @param waitTime  Max time to wait for lock (seconds)
     * @param leaseTime Lock lease time (seconds)
     * @return true if lock acquired
     */
    public boolean tryLock(String lockKey, long waitTime, long leaseTime) {
        RLock lock = redissonClient.getLock(getLockKey(lockKey));
        try {
            boolean acquired = lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
            if (acquired) {
                log.debug("Acquired lock: {}", lockKey);
            } else {
                log.debug("Failed to acquire lock: {}", lockKey);
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while acquiring lock: {}", lockKey);
            return false;
        }
    }

    /**
     * Try to acquire lock with default timeout
     */
    public boolean tryLock(String lockKey) {
        return tryLock(lockKey, DEFAULT_WAIT_TIME, DEFAULT_LEASE_TIME);
    }

    /**
     * Acquire lock (blocking)
     *
     * @param lockKey   Lock key
     * @param leaseTime Lock lease time (seconds)
     */
    public void lock(String lockKey, long leaseTime) {
        RLock lock = redissonClient.getLock(getLockKey(lockKey));
        lock.lock(leaseTime, TimeUnit.SECONDS);
        log.debug("Acquired lock: {}", lockKey);
    }

    /**
     * Acquire lock with default lease time
     */
    public void lock(String lockKey) {
        lock(lockKey, DEFAULT_LEASE_TIME);
    }

    /**
     * Release lock
     */
    public void unlock(String lockKey) {
        RLock lock = redissonClient.getLock(getLockKey(lockKey));
        if (lock.isHeldByCurrentThread()) {
            lock.unlock();
            log.debug("Released lock: {}", lockKey);
        } else {
            log.warn("Attempted to unlock a lock not held by current thread: {}", lockKey);
        }
    }

    /**
     * Check if lock is held
     */
    public boolean isLocked(String lockKey) {
        RLock lock = redissonClient.getLock(getLockKey(lockKey));
        return lock.isLocked();
    }

    /**
     * Check if lock is held by current thread
     */
    public boolean isHeldByCurrentThread(String lockKey) {
        RLock lock = redissonClient.getLock(getLockKey(lockKey));
        return lock.isHeldByCurrentThread();
    }

    /**
     * Execute action with lock
     *
     * @param lockKey   Lock key
     * @param waitTime  Wait time (seconds)
     * @param leaseTime Lease time (seconds)
     * @param action    Action to execute
     * @return Action result or null if lock not acquired
     */
    public <T> T executeWithLock(String lockKey, long waitTime, long leaseTime, Supplier<T> action) {
        if (!tryLock(lockKey, waitTime, leaseTime)) {
            log.warn("Failed to acquire lock for: {}", lockKey);
            return null;
        }
        try {
            return action.get();
        } finally {
            unlock(lockKey);
        }
    }

    /**
     * Execute action with lock (default timeout)
     */
    public <T> T executeWithLock(String lockKey, Supplier<T> action) {
        return executeWithLock(lockKey, DEFAULT_WAIT_TIME, DEFAULT_LEASE_TIME, action);
    }

    /**
     * Execute action with lock (void)
     */
    public boolean executeWithLock(String lockKey, long waitTime, long leaseTime, Runnable action) {
        if (!tryLock(lockKey, waitTime, leaseTime)) {
            log.warn("Failed to acquire lock for: {}", lockKey);
            return false;
        }
        try {
            action.run();
            return true;
        } finally {
            unlock(lockKey);
        }
    }

    /**
     * Execute action with lock (void, default timeout)
     */
    public boolean executeWithLock(String lockKey, Runnable action) {
        return executeWithLock(lockKey, DEFAULT_WAIT_TIME, DEFAULT_LEASE_TIME, action);
    }

    /**
     * Get fair lock (FIFO order)
     */
    public boolean tryFairLock(String lockKey, long waitTime, long leaseTime) {
        RLock lock = redissonClient.getFairLock(getLockKey(lockKey));
        try {
            return lock.tryLock(waitTime, leaseTime, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Get lock for task execution
     */
    public boolean tryLockTask(String taskId, long leaseTime) {
        return tryLock("task:" + taskId, DEFAULT_WAIT_TIME, leaseTime);
    }

    /**
     * Unlock task
     */
    public void unlockTask(String taskId) {
        unlock("task:" + taskId);
    }

    private String getLockKey(String lockKey) {
        return LOCK_KEY_PREFIX + lockKey;
    }
}
