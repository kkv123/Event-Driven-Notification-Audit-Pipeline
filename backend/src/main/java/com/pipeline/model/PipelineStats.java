package com.pipeline.model;

import java.util.Map;

public class PipelineStats {

    private long totalIngested;
    private long totalProcessed;
    private long totalRateLimited;
    private long totalDuplicatesBlocked;
    private long totalDlq;
    private double currentThroughputEps;
    private double averageLatencyMs;
    private int tokenBucketCapacity;
    private int tokenBucketAvailable;
    private int activeKafkaConsumers;
    private Map<String, Long> eventsByType;

    public PipelineStats() {
    }

    // Getters and Setters
    public long getTotalIngested() {
        return totalIngested;
    }

    public void setTotalIngested(long totalIngested) {
        this.totalIngested = totalIngested;
    }

    public long getTotalProcessed() {
        return totalProcessed;
    }

    public void setTotalProcessed(long totalProcessed) {
        this.totalProcessed = totalProcessed;
    }

    public long getTotalRateLimited() {
        return totalRateLimited;
    }

    public void setTotalRateLimited(long totalRateLimited) {
        this.totalRateLimited = totalRateLimited;
    }

    public long getTotalDuplicatesBlocked() {
        return totalDuplicatesBlocked;
    }

    public void setTotalDuplicatesBlocked(long totalDuplicatesBlocked) {
        this.totalDuplicatesBlocked = totalDuplicatesBlocked;
    }

    public long getTotalDlq() {
        return totalDlq;
    }

    public void setTotalDlq(long totalDlq) {
        this.totalDlq = totalDlq;
    }

    public double getCurrentThroughputEps() {
        return currentThroughputEps;
    }

    public void setCurrentThroughputEps(double currentThroughputEps) {
        this.currentThroughputEps = currentThroughputEps;
    }

    public double getAverageLatencyMs() {
        return averageLatencyMs;
    }

    public void setAverageLatencyMs(double averageLatencyMs) {
        this.averageLatencyMs = averageLatencyMs;
    }

    public int getTokenBucketCapacity() {
        return tokenBucketCapacity;
    }

    public void setTokenBucketCapacity(int tokenBucketCapacity) {
        this.tokenBucketCapacity = tokenBucketCapacity;
    }

    public int getTokenBucketAvailable() {
        return tokenBucketAvailable;
    }

    public void setTokenBucketAvailable(int tokenBucketAvailable) {
        this.tokenBucketAvailable = tokenBucketAvailable;
    }

    public int getActiveKafkaConsumers() {
        return activeKafkaConsumers;
    }

    public void setActiveKafkaConsumers(int activeKafkaConsumers) {
        this.activeKafkaConsumers = activeKafkaConsumers;
    }

    public Map<String, Long> getEventsByType() {
        return eventsByType;
    }

    public void setEventsByType(Map<String, Long> eventsByType) {
        this.eventsByType = eventsByType;
    }
}
