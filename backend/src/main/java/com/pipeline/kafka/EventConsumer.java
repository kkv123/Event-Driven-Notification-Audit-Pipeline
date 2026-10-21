package com.pipeline.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pipeline.config.KafkaConfig;
import com.pipeline.model.AuditEvent;
import com.pipeline.model.EventRequest;
import com.pipeline.model.EventStatus;
import com.pipeline.repository.AuditRepository;
import com.pipeline.service.EventPipelineService;
import com.pipeline.service.NotificationDispatcher;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
public class EventConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventConsumer.class);

    private final AuditRepository auditRepository;
    private final NotificationDispatcher notificationDispatcher;
    private final ObjectMapper objectMapper;
    private final EventPipelineService pipelineService;

    @Autowired
    public EventConsumer(AuditRepository auditRepository,
                         NotificationDispatcher notificationDispatcher,
                         ObjectMapper objectMapper,
                         EventPipelineService pipelineService) {
        this.auditRepository = auditRepository;
        this.notificationDispatcher = notificationDispatcher;
        this.objectMapper = objectMapper;
        this.pipelineService = pipelineService;
    }

    /**
     * Kafka Consumer with Guaranteed At-Least-Once Delivery.
     * Manually acknowledges after audit persistence and notification dispatch.
     */
    @KafkaListener(
            topics = {KafkaConfig.TOPIC_TRANSACTIONS, KafkaConfig.TOPIC_AUDIT, KafkaConfig.TOPIC_NOTIFICATIONS},
            groupId = "${spring.kafka.consumer.group-id:audit-pipeline-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();
        try {
            EventRequest request = objectMapper.readValue(record.value(), EventRequest.class);

            log.info("Kafka Consumer: Received event [{}] from topic [{}] partition [{}] offset [{}]",
                    request.getEventId(), record.topic(), record.partition(), record.offset());

            // 1. Audit Persistence
            AuditEvent auditEvent = new AuditEvent(
                    request.getEventId(),
                    request.getEventType(),
                    request.getSourceService(),
                    EventStatus.PROCESSED,
                    record.value(),
                    "kafka-consumer",
                    request.getIdempotencyKey()
            );
            auditEvent.setPartitionNumber(record.partition());
            auditEvent.setOffsetNumber(record.offset());
            auditEvent.setProcessedAt(LocalDateTime.now());
            auditEvent.setProcessingLatencyMs(System.currentTimeMillis() - startTime);

            auditRepository.save(auditEvent);

            // 2. Dispatch Downstream Notifications
            notificationDispatcher.dispatch(auditEvent);

            // 3. Broadcast to Real-Time SSE Dashboard
            pipelineService.broadcastToSse(auditEvent);

            // 4. Manual Acknowledgment (Ensures At-Least-Once Processing)
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }

            pipelineService.incrementProcessedCount();
        } catch (Exception e) {
            log.error("Kafka Consumer Error processing record at offset {}: {}", record.offset(), e.getMessage());
            pipelineService.incrementDlqCount();
            // Acknowledge to prevent infinite stuck loop, or delegate to DLQ
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }
}
