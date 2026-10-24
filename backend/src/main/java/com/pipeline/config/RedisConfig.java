package com.pipeline.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

@Configuration
public class RedisConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisConfig.class);

    @Value("${spring.data.redis.url:}")
    private String redisUrl;

    @Value("${spring.data.redis.host:localhost}")
    private String redisHost;

    @Value("${spring.data.redis.port:6379}")
    private int redisPort;

    @Value("${spring.data.redis.password:}")
    private String redisPassword;

    @Value("${spring.data.redis.ssl.enabled:false}")
    private boolean sslEnabled;

    @Bean
    public StringRedisTemplate stringRedisTemplate(RedisConnectionFactory connectionFactory) {
        return new StringRedisTemplate(connectionFactory);
    }

    @Bean(destroyMethod = "shutdown")
    public RedissonClient redissonClient() {
        try {
            Config config = new Config();
            String address;
            if (redisUrl != null && !redisUrl.trim().isEmpty()) {
                address = redisUrl.trim();
            } else {
                String protocol = sslEnabled ? "rediss://" : "redis://";
                address = protocol + redisHost + ":" + redisPort;
            }

            var singleServer = config.useSingleServer()
                    .setAddress(address)
                    .setConnectTimeout(5000)
                    .setTimeout(3000)
                    .setRetryAttempts(2)
                    .setRetryInterval(1500);

            if (redisPassword != null && !redisPassword.trim().isEmpty()) {
                singleServer.setPassword(redisPassword.trim());
            }

            log.info("Configuring RedissonClient connected to Redis at [{}]", address);
            return Redisson.create(config);
        } catch (Exception e) {
            log.warn("Redisson client could not connect to Redis at startup: {}", e.getMessage());
            return null;
        }
    }
}
