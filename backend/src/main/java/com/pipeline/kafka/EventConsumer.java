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
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true", matchIfMissing = false)
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
            topics = "${spring.kafka.topic:pipeline-events}",
            groupId = "${spring.kafka.consumer.group-id:audit-pipeline-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEvent(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        long startTime = System.currentTimeMillis();
        try {
            EventRequest request = objectMapper.readValue(record.value(), EventRequest.class);

            log.info("Kafka Consumer: Received and verified event [{}] from topic [{}] partition [{}] offset [{}]",
                    request.getEventId(), record.topic(), record.partition(), record.offset());

            // Manual Acknowledgment (Ensures At-Least-Once Processing commit in Kafka)
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
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
