package com.pipeline.model;

public enum EventStatus {
    INGESTED,
    PROCESSED,
    RATE_LIMITED,
    DUPLICATE_IGNORED,
    FAILED,
    DLQ
}
