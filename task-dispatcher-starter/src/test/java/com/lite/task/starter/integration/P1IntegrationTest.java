package com.lite.task.starter.integration;

import com.lite.task.common.enums.TaskStatus;
import com.lite.task.common.util.JsonUtils;
import com.lite.task.core.dispatcher.TaskDispatcher;
import com.lite.task.core.scheduler.TimeoutTaskScheduler;
import com.lite.task.domain.task.entity.TaskDefinition;
import com.lite.task.domain.task.entity.TaskInstance;
import com.lite.task.domain.task.entity.TaskOutboxEvent;
import com.lite.task.infrastructure.kafka.config.KafkaConfig;
import com.lite.task.infrastructure.persistence.repository.TaskDefinitionRepository;
import com.lite.task.infrastructure.persistence.repository.TaskExecutionLogRepository;
import com.lite.task.infrastructure.persistence.repository.TaskInstanceRepository;
import com.lite.task.infrastructure.persistence.repository.TaskOutboxEventRepository;
import com.lite.task.infrastructure.redis.DelayQueueOperator;
import com.lite.task.infrastructure.redis.TaskCacheOperator;
import com.lite.task.infrastructure.redis.TaskQueueOperator;
import com.lite.task.core.producer.TaskOutboxPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@Testcontainers(disabledWithoutDocker = true)
class P1IntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    static final GenericContainer<?> REDIS = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
            .withExposedPorts(6379);

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.5.0"));

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "update");

        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));

        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.kafka.consumer.group-id", () -> "p1-integration-group");

        registry.add("task.worker.enabled", () -> "false");
        registry.add("task.scheduler.delay.enabled", () -> "false");
        registry.add("task.scheduler.timeout.enabled", () -> "false");
        registry.add("task.scheduler.cleanup.enabled", () -> "false");
        registry.add("task.outbox.enabled", () -> "true");
        registry.add("task.outbox.scan-interval-ms", () -> "86400000");
        registry.add("task.kafka.callback.replay.enabled", () -> "false");
    }

    @Autowired
    private TaskDefinitionRepository taskDefinitionRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private TaskExecutionLogRepository taskExecutionLogRepository;

    @Autowired
    private TaskOutboxEventRepository taskOutboxEventRepository;

    @Autowired
    private TaskCacheOperator taskCacheOperator;

    @Autowired
    private TaskQueueOperator taskQueueOperator;

    @Autowired
    private DelayQueueOperator delayQueueOperator;

    @Autowired
    private TaskDispatcher taskDispatcher;

    @Autowired
    private TimeoutTaskScheduler timeoutTaskScheduler;

    @Autowired
    private TaskOutboxPublisher taskOutboxPublisher;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @BeforeEach
    void setUp() {
        taskExecutionLogRepository.deleteAll();
        taskOutboxEventRepository.deleteAll();
        taskInstanceRepository.deleteAll();
        taskDefinitionRepository.deleteAll();
        stringRedisTemplate.getConnectionFactory().getConnection().serverCommands().flushDb();
    }

    @Test
    @DisplayName("P1-4/P1-6: should execute task only once under concurrent multi-node claim")
    void shouldPreventDuplicateExecutionUnderConcurrency() throws Exception {
        TaskDefinition definition = saveDefinition("LOG", 60, 100,
                Map.of("maxAttempts", 1, "initialDelay", 200, "multiplier", 2.0, "maxDelay", 5000));
        TaskInstance task = saveTask(definition, TaskStatus.PENDING, Map.of("message", "dup-check", "delay", 200), 1, null);
        taskCacheOperator.save(task);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(1);
        Future<Boolean> first = executor.submit(() -> {
            latch.await();
            return taskDispatcher.executeTask(task.getTaskId());
        });
        Future<Boolean> second = executor.submit(() -> {
            latch.await();
            return taskDispatcher.executeTask(task.getTaskId());
        });

        latch.countDown();
        boolean firstResult = first.get(10, TimeUnit.SECONDS);
        boolean secondResult = second.get(10, TimeUnit.SECONDS);
        executor.shutdownNow();

        waitUntil(() -> taskExecutionLogRepository.countByTaskId(task.getTaskId()) >= 1, 5000);

        long logCount = taskExecutionLogRepository.countByTaskId(task.getTaskId());
        TaskInstance latest = taskInstanceRepository.findByTaskId(task.getTaskId()).orElseThrow();

        assertTrue(firstResult || secondResult);
        assertFalse(firstResult && secondResult);
        assertEquals(1, logCount);
        assertEquals(TaskStatus.SUCCESS, latest.getStatus());
    }

    @Test
    @DisplayName("P1-2/P1-6: should route failed callback consume to DLQ topic")
    void shouldSendCallbackFailureToDlq() throws Exception {
        String taskId = "kafka-callback-" + UUID.randomUUID();
        Map<String, Object> payload = new HashMap<>();
        payload.put("taskId", taskId);
        payload.put("taskType", "LOG_TASK");
        payload.put("status", "SUCCESS");
        payload.put("callbackUrl", "http://127.0.0.1:1/unreachable");
        payload.put("traceId", UUID.randomUUID().toString().replace("-", ""));

        kafkaTemplate.send(KafkaConfig.TOPIC_TASK_CALLBACK, taskId, JsonUtils.toJson(payload)).get(10, TimeUnit.SECONDS);

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "p1-dlq-check-" + UUID.randomUUID());
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        boolean found = false;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(java.util.List.of(KafkaConfig.TOPIC_TASK_DLQ));
            long deadline = System.currentTimeMillis() + 15000;
            while (System.currentTimeMillis() < deadline && !found) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null && record.value().contains(taskId)) {
                        found = true;
                        break;
                    }
                }
            }
        }

        assertTrue(found, "Expected callback failure message in DLQ topic");
    }

    @Test
    @DisplayName("P1-5/P1-6: should recover unpublished outbox event and enqueue task")
    void shouldRecoverOutboxAndEnqueueTask() throws Exception {
        TaskDefinition definition = saveDefinition("LOG", 60, 100,
                Map.of("maxAttempts", 1, "initialDelay", 200, "multiplier", 2.0, "maxDelay", 5000));
        TaskInstance task = saveTask(definition, TaskStatus.PENDING, Map.of("message", "outbox-recover"), 1, null);

        TaskOutboxEvent event = TaskOutboxEvent.builder()
                .eventType(TaskOutboxEvent.EVENT_TASK_SUBMITTED)
                .taskId(task.getTaskId())
                .status(TaskOutboxEvent.STATUS_NEW)
                .payload(Map.of("taskId", task.getTaskId(), "taskType", task.getTaskType(), "priority", task.getPriority()))
                .build();
        taskOutboxEventRepository.save(event);

        taskOutboxPublisher.recoverUnpublishedEvents();
        waitUntil(() -> {
            Optional<TaskOutboxEvent> optional = taskOutboxEventRepository.findByTaskId(task.getTaskId());
            return optional.isPresent() && TaskOutboxEvent.STATUS_PUBLISHED.equals(optional.get().getStatus());
        }, 5000);

        TaskOutboxEvent refreshed = taskOutboxEventRepository.findByTaskId(task.getTaskId()).orElseThrow();
        assertEquals(TaskOutboxEvent.STATUS_PUBLISHED, refreshed.getStatus());
        assertTrue(taskCacheOperator.exists(task.getTaskId()));
        assertTrue(taskQueueOperator.size(task.getPriorityEnum()) >= 1);
    }

    @Test
    @DisplayName("P1-4/P1-6: should move timed-out running task to retry path")
    void shouldRecoverTimedOutRunningTask() throws Exception {
        TaskDefinition definition = saveDefinition("LOG", 1, 100,
                Map.of("maxAttempts", 1, "initialDelay", 300, "multiplier", 2.0, "maxDelay", 5000));
        TaskInstance task = saveTask(definition, TaskStatus.RUNNING, Map.of("message", "timeout-recover"), 1,
                LocalDateTime.now().minusSeconds(10));
        taskQueueOperator.markRunning(task.getTaskId(), "executor-timeout", 1);
        taskCacheOperator.save(task);

        ReflectionTestUtils.setField(timeoutTaskScheduler, "enabled", true);
        timeoutTaskScheduler.scanTimedOutRunningTasks();

        waitUntil(() -> {
            TaskInstance refreshed = taskInstanceRepository.findByTaskId(task.getTaskId()).orElse(null);
            return refreshed != null && refreshed.getStatus() == TaskStatus.RETRYING;
        }, 5000);

        TaskInstance refreshed = taskInstanceRepository.findByTaskId(task.getTaskId()).orElseThrow();
        assertEquals(TaskStatus.RETRYING, refreshed.getStatus());
        assertTrue(delayQueueOperator.exists(task.getTaskId()));
        assertFalse(taskQueueOperator.isRunning(task.getTaskId()));
    }

    @Test
    @DisplayName("P1-6: should schedule retry with backoff when executor fails")
    void shouldScheduleRetryBackoffOnFailure() throws Exception {
        TaskDefinition definition = saveDefinition("LOG", 60, 100,
                Map.of("maxAttempts", 2, "initialDelay", 300, "multiplier", 2.0, "maxDelay", 5000));
        TaskInstance task = saveTask(definition, TaskStatus.PENDING, Map.of("message", "retry-backoff", "fail", true), 2, null);
        taskCacheOperator.save(task);

        boolean executed = taskDispatcher.executeTask(task.getTaskId());
        assertTrue(executed);

        waitUntil(() -> taskCacheOperator.getStatus(task.getTaskId()) == TaskStatus.RETRYING, 5000);

        assertEquals(TaskStatus.RETRYING, taskCacheOperator.getStatus(task.getTaskId()));
        assertTrue(delayQueueOperator.exists(task.getTaskId()));
        assertNotNull(delayQueueOperator.getExecuteTime(task.getTaskId()));
        assertTrue(delayQueueOperator.getExecuteTime(task.getTaskId()).isAfter(LocalDateTime.now().minusSeconds(1)));
    }

    private TaskDefinition saveDefinition(String executorType,
                                          int timeoutSeconds,
                                          int rateLimit,
                                          Map<String, Object> retryPolicy) {
        TaskDefinition definition = TaskDefinition.builder()
                .taskType("TYPE_" + UUID.randomUUID().toString().substring(0, 8))
                .taskName("Integration Definition")
                .executorType(executorType)
                .status("ENABLED")
                .timeoutSeconds(timeoutSeconds)
                .rateLimit(rateLimit)
                .executorConfig(Map.of())
                .retryPolicy(retryPolicy)
                .description("integration test")
                .build();
        return taskDefinitionRepository.save(definition);
    }

    private TaskInstance saveTask(TaskDefinition definition,
                                  TaskStatus status,
                                  Map<String, Object> params,
                                  int maxRetry,
                                  LocalDateTime startedAt) {
        LocalDateTime now = LocalDateTime.now();
        TaskInstance task = TaskInstance.builder()
                .taskId(UUID.randomUUID().toString().replace("-", ""))
                .taskDefId(definition.getId())
                .taskType(definition.getTaskType())
                .status(status)
                .priority(2)
                .params(params)
                .maxRetry(maxRetry)
                .retryCount(0)
                .executeAt(now.minusSeconds(1))
                .startedAt(startedAt)
                .createdAt(now.minusSeconds(1))
                .updatedAt(now.minusSeconds(1))
                .build();
        return taskInstanceRepository.save(task);
    }

    private void waitUntil(Condition condition, long timeoutMs) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (condition.matches()) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Condition not met within timeout: " + timeoutMs + "ms");
    }

    @FunctionalInterface
    private interface Condition {
        boolean matches() throws Exception;
    }
}
