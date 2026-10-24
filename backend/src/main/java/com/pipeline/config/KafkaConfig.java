package com.pipeline.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@ConditionalOnProperty(name = "spring.kafka.enabled", havingValue = "true", matchIfMissing = false)
public class KafkaConfig {

    public static final String TOPIC_TRANSACTIONS = "transaction-events";
    public static final String TOPIC_AUDIT = "audit-events";
    public static final String TOPIC_NOTIFICATIONS = "notification-events";
    public static final String TOPIC_DLQ = "audit-events-dlq";

    @Value("${spring.kafka.bootstrap-servers:localhost:9092}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id:audit-pipeline-group}")
    private String groupId;

    @Value("${spring.kafka.properties.security.protocol:PLAINTEXT}")
    private String securityProtocol;

    @Value("${spring.kafka.properties.sasl.mechanism:}")
    private String saslMechanism;

    @Value("${spring.kafka.properties.sasl.jaas.config:}")
    private String saslJaasConfig;

    @Value("${spring.kafka.properties.ssl.truststore.type:}")
    private String sslTruststoreType;

    @Value("${spring.kafka.properties.ssl.truststore.location:}")
    private String sslTruststoreLocation;

    @Value("${spring.kafka.properties.ssl.truststore.certificates:}")
    private String sslTruststoreCertificates;

    private void applySaslConfigs(Map<String, Object> config) {
        if (securityProtocol != null && !securityProtocol.isEmpty()) {
            config.put("security.protocol", securityProtocol);
        }
        if (saslMechanism != null && !saslMechanism.isEmpty()) {
            config.put("sasl.mechanism", saslMechanism);
        }
        if (saslJaasConfig != null && !saslJaasConfig.isEmpty()) {
            config.put("sasl.jaas.config", saslJaasConfig);
        }
        if (sslTruststoreCertificates != null && !sslTruststoreCertificates.trim().isEmpty()) {
            config.put("ssl.truststore.type", "PEM");
            config.put("ssl.truststore.certificates", sslTruststoreCertificates.replace("\\n", "\n").trim());
        } else if (sslTruststoreLocation != null && !sslTruststoreLocation.trim().isEmpty()) {
            config.put("ssl.truststore.type", "PEM");
            config.put("ssl.truststore.location", sslTruststoreLocation.trim());
        } else if (sslTruststoreType != null && !sslTruststoreType.trim().isEmpty()) {
            config.put("ssl.truststore.type", sslTruststoreType.trim());
        }
    }

    @Value("${spring.kafka.topic:pipeline-events}")
    private String configuredTopic;

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        applySaslConfigs(configs);
        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setFatalIfBrokerNotAvailable(false);
        admin.setAutoCreate(false);
        return admin;
    }

    @Bean
    public NewTopic mainEventsTopic() {
        return TopicBuilder.name(configuredTopic != null ? configuredTopic : "pipeline-events")
                .partitions(2)
                .replicas(2)
                .build();
    }

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.RETRIES_CONFIG, 2);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        applySaslConfigs(config);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        applySaslConfigs(config);
        return new DefaultKafkaConsumerFactory<>(config);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConcurrency(3);
        factory.setAutoStartup(true);
        factory.setCommonErrorHandler(new DefaultErrorHandler(new FixedBackOff(1000L, 2)));
        return factory;
    }
}
