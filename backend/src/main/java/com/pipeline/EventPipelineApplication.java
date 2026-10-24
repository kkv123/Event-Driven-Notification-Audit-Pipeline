package com.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

@SpringBootApplication
@EnableScheduling
@EnableAsync
public class EventPipelineApplication {

    private static final Logger log = LoggerFactory.getLogger(EventPipelineApplication.class);

    public static void main(String[] args) {
        loadDotenv();
        SpringApplication.run(EventPipelineApplication.class, args);
    }

    private static void loadDotenv() {
        File[] candidatePaths = new File[] {
                new File(".env"),
                new File("../.env"),
                new File("C:\\Users\\91986\\.gemini\\antigravity\\scratch\\event-pipeline\\.env")
        };

        for (File file : candidatePaths) {
            if (file.exists() && file.isFile()) {
                try {
                    List<String> lines = Files.readAllLines(file.toPath());
                    int loadedCount = 0;
                    for (String line : lines) {
                        String trimmed = line.trim();
                        if (trimmed.isEmpty() || trimmed.startsWith("#") || !trimmed.contains("=")) {
                            continue;
                        }
                        int eqIdx = trimmed.indexOf('=');
                        String key = trimmed.substring(0, eqIdx).trim();
                        String value = trimmed.substring(eqIdx + 1).trim();
                        if ((value.startsWith("\"") && value.endsWith("\"")) ||
                            (value.startsWith("'") && value.endsWith("'"))) {
                            value = value.substring(1, value.length() - 1);
                        }
                        if (System.getProperty(key) == null && System.getenv(key) == null) {
                            System.setProperty(key, value);
                            loadedCount++;
                        }
                    }
                    log.info("Successfully loaded {} variables from .env file at: {}", loadedCount, file.getAbsolutePath());
                    return;
                } catch (Exception e) {
                    log.warn("Could not read .env at {}: {}", file.getAbsolutePath(), e.getMessage());
                }
            }
        }
    }
}
