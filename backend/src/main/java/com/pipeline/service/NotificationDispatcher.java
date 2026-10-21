package com.pipeline.service;

import com.pipeline.model.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class NotificationDispatcher {

    private static final Logger log = LoggerFactory.getLogger(NotificationDispatcher.class);

    @Async
    public CompletableFuture<Void> dispatch(AuditEvent event) {
        try {
            // Simulate realistic microservice notification dispatch (e.g. Email / Webhook / In-App alert)
            long start = System.currentTimeMillis();
            Thread.sleep(15); // simulate downstream HTTP/SMTP dispatch delay

            log.info("DISPATCHER: Notification delivered for event [{}] type [{}] to subscribers in {}ms",
                    event.getEventId(), event.getEventType(), (System.currentTimeMillis() - start));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return CompletableFuture.completedFuture(null);
    }
}
