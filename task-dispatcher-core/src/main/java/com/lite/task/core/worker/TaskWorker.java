package com.lite.task.core.worker;

import com.lite.task.core.dispatcher.TaskDispatcher;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task Worker
 *
 * Continuously polls and executes tasks from the queue.
 * This component can be deployed independently as an execution terminal.
 *
 * Core execution loop:
 * 1. Poll task from priority queue
 * 2. Execute task via TaskDispatcher
 * 3. Result is handled by TaskDispatcher (success/failure/retry)
 * 4. Repeat
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskWorker {

    private final TaskDispatcher taskDispatcher;

    @Value("${task.worker.enabled:true}")
    private boolean enabled;

    @Value("${task.worker.threads:1}")
    private int workerThreads;

    @Value("${task.worker.poll-interval-ms:100}")
    private long pollIntervalMs;

    @Value("${task.worker.idle-sleep-ms:1000}")
    private long idleSleepMs;

    private ExecutorService workerExecutor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @PostConstruct
    public void start() {
        if (!enabled) {
            log.info("TaskWorker is disabled");
            return;
        }

        running.set(true);
        workerExecutor = Executors.newFixedThreadPool(workerThreads, r -> {
            Thread t = new Thread(r, "task-worker");
            t.setDaemon(false);
            return t;
        });

        for (int i = 0; i < workerThreads; i++) {
            final int workerId = i;
            workerExecutor.submit(() -> runWorkerLoop(workerId));
        }

        log.info("TaskWorker started with {} threads", workerThreads);
    }

    @PreDestroy
    public void stop() {
        if (!enabled || workerExecutor == null) {
            return;
        }

        log.info("Stopping TaskWorker...");
        running.set(false);

        workerExecutor.shutdown();
        try {
            if (!workerExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                log.warn("TaskWorker did not terminate gracefully, forcing shutdown");
                workerExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            workerExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        log.info("TaskWorker stopped");
    }

    /**
     * Main worker loop - continuously poll and execute tasks.
     */
    private void runWorkerLoop(int workerId) {
        log.info("Worker-{} started", workerId);

        while (running.get()) {
            try {
                boolean executed = taskDispatcher.pollAndExecute();

                if (executed) {
                    // Task executed, small delay before next poll
                    if (pollIntervalMs > 0) {
                        Thread.sleep(pollIntervalMs);
                    }
                } else {
                    // No task available, longer sleep to avoid busy-wait
                    Thread.sleep(idleSleepMs);
                }

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Worker-{} interrupted", workerId);
                break;
            } catch (Exception e) {
                log.error("Worker-{} error: {}", workerId, e.getMessage(), e);
                // Sleep before retry to avoid tight error loop
                try {
                    Thread.sleep(idleSleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }

        log.info("Worker-{} stopped", workerId);
    }

    /**
     * Check if worker is running.
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Get number of worker threads.
     */
    public int getWorkerThreads() {
        return workerThreads;
    }
}
