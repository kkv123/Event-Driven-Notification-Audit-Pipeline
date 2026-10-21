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
        String bucketKey = "rate_limit:bucket:" + key;

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
                    redisTemplate.opsForValue().set(tokensKey, String.valueOf(remaining));
                    redisTemplate.opsForValue().set(timestampKey, String.valueOf(nowMs));
                    return true;
                } else {
                    return false;
                }
            } catch (Exception e) {
                log.warn("Redis unavailable for rate-limiting, switching to local in-memory Token Bucket: {}", e.getMessage());
            }
        }

        // Resilient in-memory fallback
        BucketState bucket = localBuckets.computeIfAbsent(key, k -> new BucketState(capacity));
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
        BucketState bucket = localBuckets.get(key);
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
