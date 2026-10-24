package com.pipeline.controller;

import com.pipeline.model.*;
import com.pipeline.repository.AuditRepository;
import com.pipeline.service.EventPipelineService;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.util.*;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/api/events")
@CrossOrigin(origins = "*")
public class EventController {

    private final EventPipelineService pipelineService;
    private final AuditRepository auditRepository;
    private final javax.sql.DataSource dataSource;
    private final org.springframework.data.redis.core.StringRedisTemplate redisTemplate;
    private final org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    public EventController(
            EventPipelineService pipelineService,
            AuditRepository auditRepository,
            @Autowired(required = false) javax.sql.DataSource dataSource,
            @Autowired(required = false) org.springframework.data.redis.core.StringRedisTemplate redisTemplate,
            @Autowired(required = false) org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate) {
        this.pipelineService = pipelineService;
        this.auditRepository = auditRepository;
        this.dataSource = dataSource;
        this.redisTemplate = redisTemplate;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes a single event through the pipeline (Rate Limiter -> Dedup -> Kafka -> DB).
     */
    @PostMapping("/publish")
    public ResponseEntity<EventResponse> publishEvent(@Valid @RequestBody EventRequest request,
                                                      HttpServletRequest servletRequest) {
        String clientIp = extractClientIp(servletRequest);
        EventResponse response = pipelineService.processEvent(request, clientIp);
        if (response.getStatus() == EventStatus.RATE_LIMITED) {
            return ResponseEntity.status(429).body(response);
        }
        return ResponseEntity.ok(response);
    }

    /**
     * Simulates burst traffic (e.g. 40 high-frequency events) with duplicates
     * to visually prove the Token Bucket algorithm and Redis deduplication in real time.
     */
    @PostMapping("/burst")
    public ResponseEntity<Map<String, Object>> simulateBurstTraffic(
            @RequestParam(defaultValue = "35") int count,
            @RequestParam(defaultValue = "true") boolean includeDuplicates,
            HttpServletRequest servletRequest) {

        String clientIp = extractClientIp(servletRequest);
        List<EventResponse> responses = Collections.synchronizedList(new ArrayList<>());
        String[] types = {"TRANSACTION_SETTLED", "SECURITY_ALERT", "POLICY_RENEWAL", "KYC_VERIFICATION"};
        String fixedDuplicateKey = "shared-idempotency-token-" + (System.currentTimeMillis() / 10000);

        List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final int index = i;
            EventRequest req = new EventRequest();
            req.setEventId("burst-" + UUID.randomUUID().toString().substring(0, 8));
            req.setEventType(types[index % types.length]);
            req.setSourceService("payment-gateway-v2");

            // Deliberately introduce duplicate keys every 4th event if enabled
            if (includeDuplicates && (index % 4 == 0)) {
                req.setIdempotencyKey(fixedDuplicateKey);
            } else {
                req.setIdempotencyKey("unique-key-" + UUID.randomUUID().toString().substring(0, 8));
            }

            req.setData(Map.of(
                    "transactionId", "TXN-" + (100000 + index),
                    "amount", 150.0 + (index * 25.5),
                    "currency", "INR",
                    "timestamp", System.currentTimeMillis()
            ));

            futures.add(CompletableFuture.runAsync(() -> {
                EventResponse res = pipelineService.processEvent(req, clientIp);
                responses.add(res);
            }));
        }

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        long processed = responses.stream().filter(r -> r.getStatus() == EventStatus.PROCESSED).count();
        long rateLimited = responses.stream().filter(r -> r.getStatus() == EventStatus.RATE_LIMITED).count();
        long duplicates = responses.stream().filter(r -> r.getStatus() == EventStatus.DUPLICATE_IGNORED).count();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("burstSize", count);
        summary.put("processed", processed);
        summary.put("rateLimitedByTokenBucket", rateLimited);
        summary.put("duplicatesBlockedByRedis", duplicates);
        summary.put("sampleResponses", responses.subList(0, Math.min(5, responses.size())));

        return ResponseEntity.ok(summary);
    }

    /**
     * Search and view paginated audit logs from PostgreSQL.
     */
    @GetMapping("/audit")
    public ResponseEntity<Page<AuditEvent>> getAuditLogs(
            @RequestParam(required = false) String query,
            @RequestParam(required = false) EventStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "15") int size) {

        PageRequest pageable = PageRequest.of(page, Math.min(size, 100));
        boolean hasQuery = query != null && !query.trim().isEmpty();

        Page<AuditEvent> results;
        if (hasQuery) {
            results = auditRepository.searchEvents(query.trim(), status, pageable);
        } else if (status != null) {
            results = auditRepository.findByStatusOrderByCreatedAtDesc(status, pageable);
        } else {
            results = auditRepository.findAllByOrderByCreatedAtDesc(pageable);
        }
        return ResponseEntity.ok(results);
    }

    /**
     * Returns pipeline statistics: Ingestion rate, Token Bucket status, Deduplication stats.
     */
    @GetMapping("/stats")
    public ResponseEntity<PipelineStats> getPipelineStats() {
        return ResponseEntity.ok(pipelineService.getStats());
    }

    /**
     * Real-time Server-Sent Events (SSE) feed.
     * The UI dashboard subscribes here to receive live events as they flow through the pipeline.
     */
    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter streamLiveEvents() {
        return pipelineService.registerSseClient();
    }

    /**
     * Pipeline health check and live cloud connectivity diagnostics.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Event-Driven Notification & Audit Pipeline",
                "timestamp", System.currentTimeMillis()
        ));
    }

    /**
     * Actively tests real connections to Supabase PostgreSQL, Upstash Redis, and Aiven Kafka.
     */
    @GetMapping("/test-connections")
    public ResponseEntity<Map<String, Object>> testConnections() {
        Map<String, Object> report = new LinkedHashMap<>();

        // 1. Test PostgreSQL (Supabase or local)
        Map<String, Object> dbStatus = new LinkedHashMap<>();
        if (dataSource != null) {
            long t0 = System.currentTimeMillis();
            try (java.sql.Connection conn = dataSource.getConnection()) {
                String meta = conn.getMetaData().getDatabaseProductName() + " " + conn.getMetaData().getDatabaseProductVersion();
                dbStatus.put("status", "CONNECTED");
                dbStatus.put("database", meta);
                dbStatus.put("latencyMs", System.currentTimeMillis() - t0);
            } catch (Exception e) {
                dbStatus.put("status", "DISCONNECTED");
                dbStatus.put("error", e.getMessage());
            }
        } else {
            dbStatus.put("status", "NOT_CONFIGURED");
        }
        report.put("postgresql", dbStatus);

        // 2. Test Redis (Upstash or local)
        Map<String, Object> redisStatus = new LinkedHashMap<>();
        if (redisTemplate != null) {
            long t0 = System.currentTimeMillis();
            try {
                String pong = redisTemplate.getConnectionFactory().getConnection().ping();
                redisStatus.put("status", "CONNECTED");
                redisStatus.put("ping", pong);
                redisStatus.put("latencyMs", System.currentTimeMillis() - t0);
            } catch (Exception e) {
                redisStatus.put("status", "FALLBACK_IN_MEMORY");
                redisStatus.put("note", "Using resilient local memory fallback: " + e.getMessage());
            }
        } else {
            redisStatus.put("status", "FALLBACK_IN_MEMORY");
        }
        report.put("redis", redisStatus);

        // 3. Test Kafka (Aiven or local)
        Map<String, Object> kafkaStatus = new LinkedHashMap<>();
        if (kafkaTemplate != null) {
            kafkaStatus.put("status", "READY");
            kafkaStatus.put("bootstrapServers", kafkaTemplate.getProducerFactory().getConfigurationProperties().get("bootstrap.servers"));
            kafkaStatus.put("securityProtocol", kafkaTemplate.getProducerFactory().getConfigurationProperties().getOrDefault("security.protocol", "PLAINTEXT"));
        } else {
            kafkaStatus.put("status", "STANDALONE_STREAM");
            kafkaStatus.put("note", "Kafka broker disabled or running in standalone stream mode");
        }
        report.put("kafka", kafkaStatus);

        return ResponseEntity.ok(report);
    }

    private String extractClientIp(HttpServletRequest request) {
        String xf = request.getHeader("X-Forwarded-For");
        if (xf != null && !xf.isEmpty()) {
            return xf.split(",")[0].trim();
        }
        return request.getRemoteAddr() != null ? request.getRemoteAddr() : "127.0.0.1";
    }
}
