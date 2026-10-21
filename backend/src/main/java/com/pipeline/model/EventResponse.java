package com.pipeline.model;

import java.time.LocalDateTime;

public class EventResponse {

    private String eventId;
    private EventStatus status;
    private String message;
    private String eventType;
    private Long latencyMs;
    private Integer remainingTokens;
    private LocalDateTime timestamp;

    public EventResponse() {
    }

    public EventResponse(String eventId, EventStatus status, String message, String eventType, Long latencyMs) {
        this.eventId = eventId;
        this.status = status;
        this.message = message;
        this.eventType = eventType;
        this.latencyMs = latencyMs;
        this.timestamp = LocalDateTime.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public EventStatus getStatus() {
        return status;
    }

    public void setStatus(EventStatus status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getLatencyMs() {
        return latencyMs;
    }

    public void setLatencyMs(Long latencyMs) {
        this.latencyMs = latencyMs;
    }

    public Integer getRemainingTokens() {
        return remainingTokens;
    }

    public void setRemainingTokens(Integer remainingTokens) {
        this.remainingTokens = remainingTokens;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }
}
