package com.orderSystem.orderservice.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka producer configuration for the Order Service.
 *
 * <p>Configures a {@link KafkaTemplate} that publishes pre-serialized JSON strings.
 * Serialization is handled in {@link com.orderSystem.orderservice.service.OutboxRelayService}
 * before publishing, keeping Kafka concerns separate from Jackson concerns.</p>
 *
 * <p>Producer settings:
 * <ul>
 *     <li>{@code acks=all} — strongest durability guarantee (waits for all ISR acknowledgements)</li>
 *     <li>{@code retries=3} — automatic retry on transient failures</li>
 *     <li>{@code enable.idempotence=true} — prevents duplicate messages on retry</li>
 * </ul>
 * </p>
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Creates the producer factory with production-grade settings.
     *
     * @return configured Kafka producer factory
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // Durability: wait for all in-sync replicas to acknowledge
        config.put(ProducerConfig.ACKS_CONFIG, "all");

        // Idempotent producer prevents duplicate messages on retry
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        // Retry settings
        config.put(ProducerConfig.RETRIES_CONFIG, 3);
        config.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);

        // Batching for improved throughput
        config.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        return new DefaultKafkaProducerFactory<>(config);
    }

    /**
     * Creates the {@link KafkaTemplate} used to publish events.
     *
     * @return configured Kafka template
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}
