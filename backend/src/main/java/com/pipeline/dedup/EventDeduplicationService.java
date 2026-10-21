package com.pipeline.dedup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Distributed Event Deduplication Service.
 * Uses SHA-256 fingerprinting or idempotency keys with Redis SETNX (TTL 10m).
 * Falls back to an in-memory TTL map if Redis is temporarily unreachable.
 */
@Service
public class EventDeduplicationService {

    private static final Logger log = LoggerFactory.getLogger(EventDeduplicationService.class);
    private static final Duration DEDUP_TTL = Duration.ofMinutes(10);

    private final StringRedisTemplate redisTemplate;

    // Resilient In-Memory Fallback Map
    private static class CacheEntry {
        long expireTimeMs;
        CacheEntry(long ttlMs) {
            this.expireTimeMs = System.currentTimeMillis() + ttlMs;
        }
        boolean isExpired() {
            return System.currentTimeMillis() > expireTimeMs;
        }
    }

    private final ConcurrentHashMap<String, CacheEntry> localDedupCache = new ConcurrentHashMap<>();

    @Autowired(required = false)
    public EventDeduplicationService(StringRedisTemplate redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    /**
     * Checks if an event is a duplicate.
     * @param idempotencyKey explicit key from sender or null
     * @param rawPayload fallback to hash payload if no key provided
     * @return true if duplicate (already seen), false if unique (first time seen)
     */
    public boolean isDuplicate(String idempotencyKey, String rawPayload) {
        String dedupKey = generateKey(idempotencyKey, rawPayload);

        // 1. Try Redis Atomic SETNX
        if (redisTemplate != null) {
            try {
                Boolean wasSet = redisTemplate.opsForValue().setIfAbsent("dedup:" + dedupKey, "SEEN", DEDUP_TTL);
                // If wasSet is TRUE, the key did not exist before -> NOT a duplicate
                // If wasSet is FALSE, the key already existed -> DUPLICATE
                if (wasSet != null) {
                    return !wasSet;
                }
            } catch (Exception e) {
                log.warn("Redis unavailable for deduplication, using local in-memory fallback: {}", e.getMessage());
            }
        }

        // 2. Resilient In-Memory Fallback
        cleanExpiredLocalEntries();
        CacheEntry existing = localDedupCache.get(dedupKey);
        if (existing != null && !existing.isExpired()) {
            return true; // Duplicate!
        }

        // Store in local cache
        localDedupCache.put(dedupKey, new CacheEntry(DEDUP_TTL.toMillis()));
        return false;
    }

    private String generateKey(String idempotencyKey, String payload) {
        if (idempotencyKey != null && !idempotencyKey.trim().isEmpty()) {
            return "key:" + idempotencyKey.trim();
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest((payload != null ? payload : "").getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return "sha:" + hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            return "hash:" + (payload != null ? payload.hashCode() : System.currentTimeMillis());
        }
    }

    private void cleanExpiredLocalEntries() {
        if (localDedupCache.size() > 5000) {
            localDedupCache.entrySet().removeIf(entry -> entry.getValue().isExpired());
        }
    }
}
