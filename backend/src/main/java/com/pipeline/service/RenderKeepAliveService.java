package com.pipeline.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Smart Keep-Alive Service for Render Free Tier.
 * Pings the application's public health endpoint every 14 minutes
 * strictly during working hours (8:00 AM to 8:00 PM IST) to prevent
 * Render from spinning down, while allowing it to sleep at night to save free hours.
 */
@Service
public class RenderKeepAliveService {

    private static final Logger log = LoggerFactory.getLogger(RenderKeepAliveService.class);

    @Value("${render.keepalive.enabled:true}")
    private boolean enabled;

    @Value("${render.external.url:https://event-driven-notification-audit-pipeline.onrender.com/api/events/health}")
    private String healthUrl;

    @Value("${render.keepalive.timezone:Asia/Kolkata}")
    private String timezoneStr;

    private static final LocalTime START_TIME = LocalTime.of(8, 0);  // 8:00 AM
    private static final LocalTime END_TIME = LocalTime.of(20, 0);    // 8:00 PM

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .build();

    // Runs every 14 minutes (Render idle timeout is 15 minutes)
    @Scheduled(initialDelay = 60_000, fixedRate = 14 * 60 * 1000)
    public void pingSelfDuringWorkingHours() {
        if (!enabled) {
            return;
        }

        ZoneId zoneId;
        try {
            zoneId = ZoneId.of(timezoneStr);
        } catch (Exception e) {
            zoneId = ZoneId.of("Asia/Kolkata");
        }

        ZonedDateTime now = ZonedDateTime.now(zoneId);
        LocalTime currentTime = now.toLocalTime();

        // Only ping if between 08:00 AM and 08:00 PM
        if (currentTime.isAfter(START_TIME) && currentTime.isBefore(END_TIME)) {
            try {
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(healthUrl))
                        .timeout(Duration.ofSeconds(15))
                        .GET()
                        .build();

                HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                log.info("Render Keep-Alive ping sent at {} {}: status {}", currentTime, timezoneStr, response.statusCode());
            } catch (Exception e) {
                log.warn("Render Keep-Alive ping to [{}] encountered a transient issue: {}", healthUrl, e.getMessage());
            }
        } else {
            log.info("Render Keep-Alive: Current time is {} {} (outside 08:00 - 20:00). Sleeping to save Render free tier hours.",
                    currentTime, timezoneStr);
        }
    }
}
