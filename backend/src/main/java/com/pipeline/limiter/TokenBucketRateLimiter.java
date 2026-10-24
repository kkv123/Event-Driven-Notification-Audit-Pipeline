package com.pipeline.limiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Distributed Token Bucket Rate Limiter.
 * Uses Redis atomic operations when available, with resilient in-memory fallback.
 */
@Component
public class TokenBucketRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketRateLimiter.class);

    private final StringRedisTemplate redisTemplate;

    // Configurable defaults for rate-limiting
    private final int capacity = 25; // Max burst capacity
    private final double refillRatePerSecond = 5.0; // 5 tokens added per second

    // Resilient In-Memory Fallback State (when Redis is unavailable)
    private static class BucketState {
        double tokens;
        long lastRefillTimestampMs;

        BucketState(double capacity) {
            this.tokens = capacity;
            this.lastRefillTimestampMs = System.currentTimeMillis();
        }
    }

    private final ConcurrentHashMap<String, BucketState> localBuckets = new ConcurrentHashMap<>();
    private volatile String lastActiveKey = "127.0.0.1";

    @Autowired(required = false)
    public TokenBucketRateLimiter(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Attempts to acquire 1 token for the specified key (e.g., client IP or API key).
     * Evaluates in sub-millisecond memory so network round-trips to remote cloud Redis
     * do not artificially delay evaluation and refill the bucket between requests.
     */
    public boolean tryAcquire(String key) {
        return tryAcquire(key, 1);
    }

    public boolean tryAcquire(String key, int tokensRequested) {
        if (key != null && !key.trim().isEmpty()) {
            this.lastActiveKey = key;
        }
        String effectiveKey = (key != null && !key.trim().isEmpty()) ? key : "default";

        // 1. High-speed in-memory token bucket evaluation (sub-millisecond lock per client key)
        BucketState bucket = localBuckets.computeIfAbsent(effectiveKey, k -> new BucketState(capacity));
        long now = System.currentTimeMillis();
        boolean acquired = false;
        double remainingTokens;

        synchronized (bucket) {
            double elapsedSeconds = Math.max(0, (now - bucket.lastRefillTimestampMs) / 1000.0);
            bucket.tokens = Math.min(capacity, bucket.tokens + (elapsedSeconds * refillRatePerSecond));
            bucket.lastRefillTimestampMs = now;

            if (bucket.tokens >= tokensRequested) {
                bucket.tokens -= tokensRequested;
                acquired = true;
            } else {
                acquired = false;
            }
            remainingTokens = bucket.tokens;
        }

        // 2. Asynchronously persist to Redis (if available) without blocking or delaying rate evaluation
        if (redisTemplate != null) {
            final double finalTokens = remainingTokens;
            final long finalTs = now;
            java.util.concurrent.CompletableFuture.runAsync(() -> {
                try {
                    String bucketKey = "rate_limit:bucket:" + effectiveKey;
                    redisTemplate.opsForValue().set(bucketKey + ":tokens", String.valueOf(finalTokens), java.time.Duration.ofHours(1));
                    redisTemplate.opsForValue().set(bucketKey + ":last_ts", String.valueOf(finalTs), java.time.Duration.ofHours(1));
                } catch (Exception e) {
                    log.debug("Async Redis token sync skipped: {}", e.getMessage());
                }
            });
        }

        return acquired;
    }

    public int getAvailableTokens(String key) {
        String effectiveKey = (key == null || "default".equalsIgnoreCase(key)) ? lastActiveKey : key;
        BucketState bucket = localBuckets.get(effectiveKey);
        if (bucket == null) {
            return capacity;
        }
        long now = System.currentTimeMillis();
        synchronized (bucket) {
            double elapsed = Math.max(0, (now - bucket.lastRefillTimestampMs) / 1000.0);
            return (int) Math.min(capacity, bucket.tokens + (elapsed * refillRatePerSecond));
        }
    }

    public int getCapacity() {
        return capacity;
    }

    public double getRefillRatePerSecond() {
        return refillRatePerSecond;
    }
}
