package com.lite.task.infrastructure.kafka.consumer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.lite.task.common.util.JsonUtils;
import com.lite.task.common.util.TraceIdHolder;
import com.lite.task.infrastructure.kafka.config.KafkaConfig;
import com.lite.task.infrastructure.kafka.producer.TaskEventProducer;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Kafka consumers for callback handling and DLQ replay.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskKafkaConsumer {

    private final TaskEventProducer taskEventProducer;
    private final MeterRegistry meterRegistry;

    @Value("${task.kafka.callback.http-timeout-ms:5000}")
    private long callbackHttpTimeoutMs;

    @Value("${task.kafka.callback.replay.enabled:true}")
    private boolean replayEnabled;

    @Value("${task.kafka.callback.replay.max-attempts:3}")
    private int replayMaxAttempts;

    @Value("${task.kafka.callback.replay.base-delay-ms:1000}")
    private long replayBaseDelayMs;

    @Value("${task.kafka.callback.replay.max-delay-ms:60000}")
    private long replayMaxDelayMs;

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private final ScheduledExecutorService replayScheduler = Executors.newSingleThreadScheduledExecutor(new ReplayThreadFactory());

    @KafkaListener(
            topics = KafkaConfig.TOPIC_TASK_CALLBACK,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onCallback(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        Map<String, Object> payload = parseMap(record.value());
        String taskId = coalesceString(payload.get("taskId"), record.key(), "unknown-task");
        String traceId = coalesceString(payload.get("traceId"), null, null);
        TraceIdHolder.bindTraceId(traceId);
        TraceIdHolder.bindTaskId(taskId);

        boolean ack = false;
        try {
            processCallback(taskId, payload);
            meterRegistry.counter("task.callback.consume.total", "status", "success").increment();
            log.info("Consumed callback event successfully: taskId={}, offset={}", taskId, record.offset());
            ack = true;
        } catch (Exception e) {
            meterRegistry.counter("task.callback.consume.total", "status", "failed").increment();
            try {
                taskEventProducer.publishToDlqSync(taskId, payload, e.getMessage(), KafkaConfig.TOPIC_TASK_CALLBACK);
                log.warn("Callback consume failed and sent to DLQ: taskId={}, error={}", taskId, e.getMessage());
                ack = true;
            } catch (Exception dlqError) {
                log.error("Callback consume failed and DLQ publish failed, will retry by Kafka: taskId={}, error={}",
                        taskId, dlqError.getMessage(), dlqError);
                throw dlqError;
            }
        } finally {
            if (ack) {
                acknowledgment.acknowledge();
            }
            TraceIdHolder.clearTaskId();
            TraceIdHolder.clearTraceId();
        }
    }

    @KafkaListener(
            topics = KafkaConfig.TOPIC_TASK_DLQ,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onDlq(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        Map<String, Object> dlqPayload = parseMap(record.value());
        String taskId = coalesceString(dlqPayload.get("taskId"), record.key(), "unknown-task");
        TraceIdHolder.bindTraceId(null);
        TraceIdHolder.bindTaskId(taskId);

        try {
            handleDlqMessage(taskId, dlqPayload);
            acknowledgment.acknowledge();
        } finally {
            TraceIdHolder.clearTaskId();
            TraceIdHolder.clearTraceId();
        }
    }

    @PreDestroy
    public void shutdownScheduler() {
        replayScheduler.shutdownNow();
    }

    private void processCallback(String taskId, Map<String, Object> payload) throws Exception {
        String callbackUrl = coalesceString(payload.get("callbackUrl"), null, null);
        if (callbackUrl == null) {
            throw new IllegalArgumentException("callbackUrl is missing");
        }

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(callbackUrl))
                .header("Content-Type", "application/json")
                .header("X-Task-Id", taskId)
                .header("X-Trace-Id", TraceIdHolder.getOrCreateTraceId())
                .timeout(Duration.ofMillis(Math.max(1000, callbackHttpTimeoutMs)))
                .POST(HttpRequest.BodyPublishers.ofString(JsonUtils.toJson(payload)))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IllegalStateException("Callback HTTP status is non-2xx: " + response.statusCode());
        }
    }

    private void handleDlqMessage(String taskId, Map<String, Object> dlqPayload) {
        if (!replayEnabled) {
            meterRegistry.counter("task.dlq.replay.total", "result", "disabled").increment();
            return;
        }

        String sourceTopic = coalesceString(dlqPayload.get("sourceTopic"), null, "unknown");
        if (!Objects.equals(sourceTopic, KafkaConfig.TOPIC_TASK_CALLBACK)) {
            meterRegistry.counter("task.dlq.replay.total", "result", "ignored").increment();
            return;
        }

        Map<String, Object> originalMessage = extractOriginalMessage(dlqPayload.get("originalMessage"));
        int replayCount = extractReplayCount(dlqPayload, originalMessage);
        if (replayCount >= replayMaxAttempts) {
            meterRegistry.counter("task.dlq.replay.total", "result", "dropped").increment();
            log.error("Drop DLQ message after max replay attempts: taskId={}, replayCount={}", taskId, replayCount);
            return;
        }

        int nextReplayCount = replayCount + 1;
        originalMessage.put("taskId", taskId);
        originalMessage.put("traceId", TraceIdHolder.getOrCreateTraceId());
        originalMessage.put("dlqReplayCount", nextReplayCount);
        originalMessage.put("dlqLastReason", coalesceString(dlqPayload.get("reason"), null, "unknown"));

        long delayMs = calculateReplayDelayMs(replayCount);
        replayScheduler.schedule(() -> {
            TraceIdHolder.bindTraceId(coalesceString(originalMessage.get("traceId"), null, null));
            TraceIdHolder.bindTaskId(taskId);
            try {
                taskEventProducer.publishCallback(taskId, originalMessage);
                meterRegistry.counter("task.dlq.replay.total", "result", "scheduled").increment();
                log.info("Replay DLQ message to callback topic: taskId={}, replayCount={}, delayMs={}",
                        taskId, nextReplayCount, delayMs);
            } finally {
                TraceIdHolder.clearTaskId();
                TraceIdHolder.clearTraceId();
            }
        }, delayMs, TimeUnit.MILLISECONDS);
    }

    private long calculateReplayDelayMs(int replayCount) {
        long safeBase = Math.max(1000L, replayBaseDelayMs);
        long shifted;
        if (replayCount >= 30) {
            shifted = Integer.MAX_VALUE;
        } else {
            shifted = 1L << replayCount;
        }
        long calculated = safeBase * shifted;
        return Math.min(calculated, Math.max(replayMaxDelayMs, safeBase));
    }

    private Map<String, Object> parseMap(String json) {
        if (json == null || json.isBlank()) {
            return new HashMap<>();
        }
        try {
            Map<String, Object> map = JsonUtils.fromJson(json, new TypeReference<Map<String, Object>>() {
            });
            return map != null ? map : new HashMap<>();
        } catch (Exception e) {
            log.warn("Failed to parse Kafka payload as JSON map: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    private Map<String, Object> extractOriginalMessage(Object source) {
        if (source instanceof Map<?, ?> map) {
            Map<String, Object> result = new HashMap<>();
            map.forEach((key, value) -> result.put(String.valueOf(key), value));
            return result;
        }
        if (source instanceof String json && JsonUtils.isValidJson(json)) {
            return parseMap(json);
        }
        return new HashMap<>();
    }

    private int extractReplayCount(Map<String, Object> dlqPayload, Map<String, Object> originalMessage) {
        Object explicit = dlqPayload.get("replayCount");
        if (explicit instanceof Number number) {
            return number.intValue();
        }
        Object nested = originalMessage.get("dlqReplayCount");
        if (nested instanceof Number number) {
            return number.intValue();
        }
        return 0;
    }

    private String coalesceString(Object primary, Object fallback, String defaultValue) {
        String first = asString(primary);
        if (first != null) {
            return first;
        }
        String second = asString(fallback);
        if (second != null) {
            return second;
        }
        return defaultValue;
    }

    private String asString(Object value) {
        if (value == null) {
            return null;
        }
        String text = String.valueOf(value).trim();
        return text.isEmpty() ? null : text;
    }

    private static class ReplayThreadFactory implements ThreadFactory {
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, "kafka-dlq-replay");
            thread.setDaemon(true);
            return thread;
        }
    }
}
