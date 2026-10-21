package com.pipeline.model;

import jakarta.validation.constraints.NotBlank;
import java.util.Map;

public class EventRequest {

    private String eventId;

    @NotBlank(message = "eventType cannot be blank")
    private String eventType;

    @NotBlank(message = "sourceService cannot be blank")
    private String sourceService;

    private String idempotencyKey;

    private Map<String, Object> data;

    public EventRequest() {
    }

    public EventRequest(String eventId, String eventType, String sourceService, String idempotencyKey, Map<String, Object> data) {
        this.eventId = eventId;
        this.eventType = eventType;
        this.sourceService = sourceService;
        this.idempotencyKey = idempotencyKey;
        this.data = data;
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

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public void setData(Map<String, Object> data) {
        this.data = data;
    }
}
