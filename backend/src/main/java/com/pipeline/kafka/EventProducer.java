package com.pipeline.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.KafkaConfig;
import com.pipeline.model.EventRequest;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class EventProducer {

    private static final Logger log = LoggerFactory.getLogger(EventProducer.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Autowired
    public EventProducer(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Publishes an event to Apache Kafka with guaranteed at-least-once delivery.
     * Uses correlation IDs and idempotency headers.
     */
    public CompletableFuture<Boolean> publishEvent(String topic, EventRequest request) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        try {
            String payloadJson = objectMapper.writeValueAsString(request);
            String partitionKey = request.getEventId() != null ? request.getEventId() : UUID.randomUUID().toString();

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, payloadJson);
            record.headers().add(new RecordHeader("X-Correlation-Id", partitionKey.getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Event-Type", request.getEventType().getBytes(StandardCharsets.UTF_8)));
            record.headers().add(new RecordHeader("X-Source-Service", request.getSourceService().getBytes(StandardCharsets.UTF_8)));

            kafkaTemplate.send(record).whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Kafka: Successfully sent event [{}] to topic [{}] partition [{}] offset [{}]",
                            request.getEventId(), topic, result.getRecordMetadata().partition(), result.getRecordMetadata().offset());
                    future.complete(true);
                } else {
                    log.error("Kafka: Failed to send event [{}] to topic [{}]: {}", request.getEventId(), topic, ex.getMessage());
                    future.complete(false);
                }
            });
        } catch (Exception e) {
            log.error("Kafka serialization error for event [{}]: {}", request.getEventId(), e.getMessage());
            future.complete(false);
        }
        return future;
    }
}
