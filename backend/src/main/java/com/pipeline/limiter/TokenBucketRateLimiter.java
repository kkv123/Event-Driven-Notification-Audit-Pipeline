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
     * @return true if token was acquired (request allowed), false if rate-limited.
     */
    public boolean tryAcquire(String key) {
        return tryAcquire(key, 1);
    }

    public synchronized boolean tryAcquire(String key, int tokensRequested) {
        if (key != null && !key.trim().isEmpty()) {
            this.lastActiveKey = key;
        }
        String bucketKey = "rate_limit:bucket:" + (key != null ? key : "default");

        // Try Redis first
        if (redisTemplate != null) {
            try {
                long nowMs = System.currentTimeMillis();
                String tokensKey = bucketKey + ":tokens";
                String timestampKey = bucketKey + ":last_ts";

                String currentTokensStr = redisTemplate.opsForValue().get(tokensKey);
                String lastTsStr = redisTemplate.opsForValue().get(timestampKey);

                double currentTokens = currentTokensStr != null ? Double.parseDouble(currentTokensStr) : capacity;
                long lastTs = lastTsStr != null ? Long.parseLong(lastTsStr) : nowMs;

                // Calculate refilled tokens based on elapsed time
                double elapsedSec = Math.max(0, (nowMs - lastTs) / 1000.0);
                double refilledTokens = Math.min(capacity, currentTokens + (elapsedSec * refillRatePerSecond));

                if (refilledTokens >= tokensRequested) {
                    double remaining = refilledTokens - tokensRequested;
                    redisTemplate.opsForValue().set(tokensKey, String.valueOf(remaining), java.time.Duration.ofHours(1));
                    redisTemplate.opsForValue().set(timestampKey, String.valueOf(nowMs), java.time.Duration.ofHours(1));
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                log.warn("Redis unavailable for rate-limiting, switching to local in-memory Token Bucket: {}", e.getMessage());
            }
        }

        // Resilient in-memory fallback
        BucketState bucket = localBuckets.computeIfAbsent(key != null ? key : "default", k -> new BucketState(capacity));
        long now = System.currentTimeMillis();
        synchronized (bucket) {
            double elapsedSeconds = Math.max(0, (now - bucket.lastRefillTimestampMs) / 1000.0);
            bucket.tokens = Math.min(capacity, bucket.tokens + (elapsedSeconds * refillRatePerSecond));
            bucket.lastRefillTimestampMs = now;

            if (bucket.tokens >= tokensRequested) {
                bucket.tokens -= tokensRequested;
                return true;
            } else {
                return false;
            }
        }
    }

    public int getAvailableTokens(String key) {
        String effectiveKey = (key == null || "default".equalsIgnoreCase(key)) ? lastActiveKey : key;
        String bucketKey = "rate_limit:bucket:" + effectiveKey;

        if (redisTemplate != null) {
            try {
                String tokensKey = bucketKey + ":tokens";
                String timestampKey = bucketKey + ":last_ts";

                String currentTokensStr = redisTemplate.opsForValue().get(tokensKey);
                String lastTsStr = redisTemplate.opsForValue().get(timestampKey);

                if (currentTokensStr != null && lastTsStr != null) {
                    double currentTokens = Double.parseDouble(currentTokensStr);
                    long lastTs = Long.parseLong(lastTsStr);
                    long nowMs = System.currentTimeMillis();
                    double elapsedSec = Math.max(0, (nowMs - lastTs) / 1000.0);
                    return (int) Math.min(capacity, currentTokens + (elapsedSec * refillRatePerSecond));
                }
            } catch (Exception e) {
                log.debug("Redis query for remaining tokens fell back to memory: {}", e.getMessage());
            }
        }

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
