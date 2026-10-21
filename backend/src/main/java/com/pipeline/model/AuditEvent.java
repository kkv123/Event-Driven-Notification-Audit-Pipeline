package com.pipeline.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "audit_events", indexes = {
    @Index(name = "idx_event_id", columnList = "eventId"),
    @Index(name = "idx_event_type", columnList = "eventType"),
    @Index(name = "idx_status", columnList = "status"),
    @Index(name = "idx_created_at", columnList = "createdAt")
})
public class AuditEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 64)
    private String eventId;

    @Column(nullable = false, length = 64)
    private String eventType;

    @Column(nullable = false, length = 64)
    private String sourceService;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private EventStatus status;

    @Column(columnDefinition = "TEXT")
    private String payload;

    private String clientIp;

    private String idempotencyKey;

    private Integer partitionNumber;

    private Long offsetNumber;

    private Long processingLatencyMs;

    @Column(nullable = false)
    private LocalDateTime createdAt;

    private LocalDateTime processedAt;

    public AuditEvent() {
    }

    public AuditEvent(String eventId, String eventType, String sourceService, EventStatus status,
                      String payload, String clientIp, String idempotencyKey) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.sourceService = sourceService;
        this.status = status;
        this.payload = payload;
        this.clientIp = clientIp;
        this.idempotencyKey = idempotencyKey;
        this.createdAt = LocalDateTime.now();
    }

    // Getters and Setters
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getSourceService() {
        return sourceService;
    }

    public void setSourceService(String sourceService) {
        this.sourceService = sourceService;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public String getClientIp() {
        return clientIp;
    }

    public void setClientIp(String clientIp) {
        this.clientIp = clientIp;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Integer getPartitionNumber() {
        return partitionNumber;
    }

    public void setPartitionNumber(Integer partitionNumber) {
        this.partitionNumber = partitionNumber;
    }

    public Long getOffsetNumber() {
        return offsetNumber;
    }

    public void setOffsetNumber(Long offsetNumber) {
        this.offsetNumber = offsetNumber;
    }

    public Long getProcessingLatencyMs() {
        return processingLatencyMs;
    }

    public void setProcessingLatencyMs(Long processingLatencyMs) {
        this.processingLatencyMs = processingLatencyMs;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(LocalDateTime processedAt) {
        this.processedAt = processedAt;
    }
}
