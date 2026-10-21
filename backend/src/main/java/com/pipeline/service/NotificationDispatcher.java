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
        log.info("DISPATCHER: Notification dispatched for event [{}] type [{}]",
                event.getEventId(), event.getEventType());
        return CompletableFuture.completedFuture(null);
    }
}
