package com.orderSystem.paymentservice.kafka.config;

import com.orderSystem.shared.config.KafkaTopics;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * Kafka configuration for the Payment Service.
 *
 * <p>Key features configured here:
 * <ul>
 *     <li><b>Manual acknowledgement</b>: offsets are committed only after successful processing.</li>
 *     <li><b>Retry with backoff</b>: failed messages are retried 3 times with a 2-second interval.</li>
 *     <li><b>Dead Letter Queue</b>: after all retries are exhausted, messages are published
 *         to {@code order.created.dlq} for investigation and manual replay.</li>
 * </ul>
 * </p>
 */
@Configuration
public class KafkaConsumerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    // -------------------------------------------------------------------------
    // Consumer Factory
    // -------------------------------------------------------------------------

    /**
     * Creates the consumer factory with manual offset acknowledgement.
     */
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Manual commit: don't auto-commit offsets
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Start from the earliest message if no offset is committed
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Fetch settings
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

        return new DefaultKafkaConsumerFactory<>(config);
    }

    // -------------------------------------------------------------------------
    // Producer Factory (needed for DLQ publisher)
    // -------------------------------------------------------------------------

    @Bean
    public ProducerFactory<String, String> paymentProducerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(paymentProducerFactory());
    }

    // -------------------------------------------------------------------------
    // Error Handler with Retry + DLQ
    // -------------------------------------------------------------------------

    /**
     * Configures the error handler with:
     * <ul>
     *     <li>3 retry attempts with 2-second intervals (FixedBackOff)</li>
     *     <li>Dead Letter Queue publishing after all retries are exhausted</li>
     * </ul>
     *
     * <p>Failed messages are routed to {@code order.created.dlq} for manual inspection.</p>
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> kafkaTemplate) {
        // DLQ recoverer: routes failed messages to the DLQ topic
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    log.error("Sending message to DLQ after all retries exhausted. " +
                                    "Topic: {} | Key: {} | Error: {}",
                            record.topic(), record.key(), ex.getMessage());
                    return new org.apache.kafka.common.TopicPartition(
                            KafkaTopics.ORDER_CREATED_DLQ, 0);
                }
        );

        // Retry 3 times with 2000ms fixed backoff
        FixedBackOff backOff = new FixedBackOff(2000L, 3L);
        return new DefaultErrorHandler(recoverer, backOff);
    }

    // -------------------------------------------------------------------------
    // Listener Container Factory
    // -------------------------------------------------------------------------

    /**
     * Creates the listener container factory with MANUAL_IMMEDIATE acknowledgement mode.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            DefaultErrorHandler errorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler);

        // MANUAL_IMMEDIATE: application controls when to commit offsets
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // Process up to 3 partitions concurrently (match topic partition count)
        factory.setConcurrency(3);

        return factory;
    }

    // -------------------------------------------------------------------------
    // Topic Declarations
    // -------------------------------------------------------------------------

    @Bean
    public org.apache.kafka.clients.admin.NewTopic paymentCompletedTopic() {
        return org.springframework.kafka.config.TopicBuilder
                .name(KafkaTopics.PAYMENT_COMPLETED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public org.apache.kafka.clients.admin.NewTopic paymentCompletedDlqTopic() {
        return org.springframework.kafka.config.TopicBuilder
                .name(KafkaTopics.PAYMENT_COMPLETED_DLQ)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
