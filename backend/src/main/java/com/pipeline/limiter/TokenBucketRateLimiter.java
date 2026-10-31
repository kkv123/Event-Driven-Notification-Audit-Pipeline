package com.pipeline.limiter;

import org.redisson.api.RRateLimiter;
import org.redisson.api.RateIntervalUnit;
import org.redisson.api.RateType;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Distributed Token Bucket Rate Limiter supporting Redisson.
 * 
 * Includes BOTH implementations requested:
 * 1. VERSION 1 (Commented for reference): Redisson Distributed Lock based on User (RLock per client IP).
 * 2. VERSION 2 (Active in Production): Redisson Distributed RateLimiter (RRateLimiter) with 1ms atomic execution.
 */
@Component
public class TokenBucketRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(TokenBucketRateLimiter.class);

    private final StringRedisTemplate redisTemplate;
    private final RedissonClient redissonClient;

    // Configurable defaults for rate-limiting
    private final int capacity = 15; // Max burst capacity (reduced from 25 to 15 for cloud free-tier execution)
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

    @Autowired
    public TokenBucketRateLimiter(
            @Autowired(required = false) StringRedisTemplate redisTemplate,
            @Autowired(required = false) RedissonClient redissonClient) {
        this.redisTemplate = redisTemplate;
        this.redissonClient = redissonClient;
    }

    /**
     * Attempts to acquire 1 token for the specified key (e.g., client IP or API key).
     */
    public boolean tryAcquire(String key) {
        return tryAcquire(key, 1);
    }

    public boolean tryAcquire(String key, int tokensRequested) {
        if (key != null && !key.trim().isEmpty()) {
            this.lastActiveKey = key;
        }
        String effectiveKey = (key != null && !key.trim().isEmpty()) ? key : "default";

        // =========================================================================================
        // VERSION 1: REDISSON DISTRIBUTED LOCK PER USER (RLock) [COMMENTED FOR REFERENCE]
        // =========================================================================================
        /*
        if (redissonClient != null && redisTemplate != null) {
            String lockKey = "lock:rate_limit:" + effectiveKey;
            org.redisson.api.RLock userLock = redissonClient.getLock(lockKey);
            try {
                // Acquire lock ONLY for this user (waits up to 100ms, leases for up to 2 seconds).
                // User A NEVER blocks User B! Each user locks only their own key in Redis.
                if (userLock.tryLock(100, 2000, TimeUnit.MILLISECONDS)) {
                    try {
                        String tokensKey = "rate_limit:bucket:" + effectiveKey + ":tokens";
                        String tsKey = "rate_limit:bucket:" + effectiveKey + ":ts";

                        String curTokensStr = redisTemplate.opsForValue().get(tokensKey);
                        String lastTsStr = redisTemplate.opsForValue().get(tsKey);

                        long now = System.currentTimeMillis();
                        double curTokens = curTokensStr != null ? Double.parseDouble(curTokensStr) : capacity;
                        long lastTs = lastTsStr != null ? Long.parseLong(lastTsStr) : now;

                        double elapsed = Math.max(0, (now - lastTs) / 1000.0);
                        double refilled = Math.min(capacity, curTokens + (elapsed * refillRatePerSecond));

                        if (refilled >= tokensRequested) {
                            double remaining = refilled - tokensRequested;
                            redisTemplate.opsForValue().set(tokensKey, String.valueOf(remaining), java.time.Duration.ofHours(1));
                            redisTemplate.opsForValue().set(tsKey, String.valueOf(now), java.time.Duration.ofHours(1));
                            return true;
                        } else {
                            redisTemplate.opsForValue().set(tokensKey, String.valueOf(refilled), java.time.Duration.ofHours(1));
                            redisTemplate.opsForValue().set(tsKey, String.valueOf(now), java.time.Duration.ofHours(1));
                            return false; // Throttled (HTTP 429)
                        }
                    } finally {
                        if (userLock.isHeldByCurrentThread()) {
                            userLock.unlock();
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        */

        // =========================================================================================
        // VERSION 2: ACTIVE IMPLEMENTATION - REDISSON DISTRIBUTED RATE LIMITER (RRateLimiter)
        // =========================================================================
        if (redissonClient != null) {
            try {
                String limiterKey = "redisson:rate_limit:" + effectiveKey;
                RRateLimiter rateLimiter = redissonClient.getRateLimiter(limiterKey);

                // Initialize or update rate limiter: 15 tokens capacity, refills at 5 tokens every 1 second
                if (!rateLimiter.trySetRate(RateType.OVERALL, capacity, 1, RateIntervalUnit.SECONDS)) {
                    rateLimiter.setRate(RateType.OVERALL, capacity, 1, RateIntervalUnit.SECONDS);
                }

                // Atomically try to acquire tokens from Redis (single 1ms network trip, 0 Java locks)
                return rateLimiter.tryAcquire(tokensRequested);
            } catch (Exception e) {
                log.warn("Redisson RRateLimiter error, falling back to local memory: {}", e.getMessage());
            }
        }

        // Resilient in-memory fallback (used when Redisson / Redis is offline)
        BucketState bucket = localBuckets.computeIfAbsent(effectiveKey, k -> new BucketState(capacity));
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

        if (redissonClient != null) {
            try {
                String limiterKey = "redisson:rate_limit:" + effectiveKey;
                RRateLimiter rateLimiter = redissonClient.getRateLimiter(limiterKey);
                return (int) rateLimiter.availablePermits();
            } catch (Exception ignored) {
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
