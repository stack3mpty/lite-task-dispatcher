package com.lite.task.infrastructure.kafka.producer;

import com.lite.task.common.util.JsonUtils;
import com.lite.task.domain.task.event.*;
import com.lite.task.infrastructure.kafka.config.KafkaConfig;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * Task Event Producer
 *
 * Publishes task events to Kafka topics
 *
 * @author lite-task-dispatcher
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskEventProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Publish task created event
     */
    public void publishTaskCreated(TaskCreatedEvent event) {
        publish(KafkaConfig.TOPIC_TASK_CREATED, event.getTaskId(), event);
    }

    /**
     * Publish task started event
     */
    public void publishTaskStarted(TaskStartedEvent event) {
        publish(KafkaConfig.TOPIC_TASK_STARTED, event.getTaskId(), event);
    }

    /**
     * Publish task completed event
     */
    public void publishTaskCompleted(TaskCompletedEvent event) {
        publish(KafkaConfig.TOPIC_TASK_COMPLETED, event.getTaskId(), event);
    }

    /**
     * Publish task failed event
     */
    public void publishTaskFailed(TaskFailedEvent event) {
        publish(KafkaConfig.TOPIC_TASK_FAILED, event.getTaskId(), event);
    }

    /**
     * Publish generic task event
     */
    public void publishEvent(TaskEvent event) {
        String topic = switch (event.getEventType()) {
            case "TASK_CREATED" -> KafkaConfig.TOPIC_TASK_CREATED;
            case "TASK_STARTED" -> KafkaConfig.TOPIC_TASK_STARTED;
            case "TASK_COMPLETED" -> KafkaConfig.TOPIC_TASK_COMPLETED;
            case "TASK_FAILED" -> KafkaConfig.TOPIC_TASK_FAILED;
            default -> throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
        };
        publish(topic, event.getTaskId(), event);
    }

    /**
     * Publish callback message
     */
    public void publishCallback(String taskId, Object callbackData) {
        publish(KafkaConfig.TOPIC_TASK_CALLBACK, taskId, callbackData);
    }

    /**
     * Publish to dead letter queue
     */
    public void publishToDlq(String taskId, Object message, String reason) {
        DlqMessage dlqMessage = new DlqMessage(taskId, message, reason);
        publish(KafkaConfig.TOPIC_TASK_DLQ, taskId, dlqMessage);
    }

    /**
     * Generic publish method
     */
    private void publish(String topic, String key, Object message) {
        String json = JsonUtils.toJson(message);
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, json);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Failed to send message to topic: {}, key: {}, error: {}",
                        topic, key, ex.getMessage(), ex);
            } else {
                log.debug("Sent message to topic: {}, key: {}, partition: {}, offset: {}",
                        topic, key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });
    }

    /**
     * Synchronous publish (waits for acknowledgment)
     */
    public void publishSync(String topic, String key, Object message) {
        String json = JsonUtils.toJson(message);
        try {
            SendResult<String, String> result = kafkaTemplate.send(topic, key, json).get();
            log.debug("Sent message sync to topic: {}, key: {}, partition: {}, offset: {}",
                    topic, key,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        } catch (Exception e) {
            log.error("Failed to send message sync to topic: {}, key: {}, error: {}",
                    topic, key, e.getMessage(), e);
            throw new RuntimeException("Failed to send message to Kafka", e);
        }
    }

    /**
     * Dead Letter Queue message wrapper
     */
    private record DlqMessage(String taskId, Object originalMessage, String reason) {}
}
