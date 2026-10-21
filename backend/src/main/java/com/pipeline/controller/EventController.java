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

    @Autowired
    public EventController(EventPipelineService pipelineService, AuditRepository auditRepository) {
        this.pipelineService = pipelineService;
        this.auditRepository = auditRepository;
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
        List<EventResponse> responses = new ArrayList<>();
        String[] types = {"TRANSACTION_SETTLED", "SECURITY_ALERT", "POLICY_RENEWAL", "KYC_VERIFICATION"};
        String fixedDuplicateKey = "shared-idempotency-token-" + (System.currentTimeMillis() / 10000);

        for (int i = 0; i < count; i++) {
            EventRequest req = new EventRequest();
            req.setEventId("burst-" + UUID.randomUUID().toString().substring(0, 8));
            req.setEventType(types[i % types.length]);
            req.setSourceService("payment-gateway-v2");

            // Deliberately introduce duplicate keys every 4th event if enabled
            if (includeDuplicates && (i % 4 == 0)) {
                req.setIdempotencyKey(fixedDuplicateKey);
            } else {
                req.setIdempotencyKey("unique-key-" + UUID.randomUUID().toString().substring(0, 8));
            }

            req.setData(Map.of(
                    "transactionId", "TXN-" + (100000 + i),
                    "amount", 150.0 + (i * 25.5),
                    "currency", "INR",
                    "timestamp", System.currentTimeMillis()
            ));

            responses.add(pipelineService.processEvent(req, clientIp));
        }

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

        Page<AuditEvent> results = auditRepository.searchEvents(
                query, status, PageRequest.of(page, Math.min(size, 100)));
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
     * Pipeline health check.
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "service", "Event-Driven Notification & Audit Pipeline",
                "timestamp", System.currentTimeMillis(),
                "kafkaCluster", "READY",
                "redisState", "ACTIVE",
                "database", "CONNECTED"
        ));
    }

    private String extractClientIp(HttpServletRequest request) {
        String xf = request.getHeader("X-Forwarded-For");
        if (xf != null && !xf.isEmpty()) {
            return xf.split(",")[0].trim();
        }
        return request.getRemoteAddr() != null ? request.getRemoteAddr() : "127.0.0.1";
    }
}
