package com.orderSystem.orderservice.kafka.config;

import com.orderSystem.shared.config.KafkaTopics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares Kafka topics managed by the Order Service.
 *
 * <p>Spring Kafka automatically creates these topics at startup if they don't exist.
 * In production, topics should be pre-created with proper replication factors.</p>
 */
@Configuration
public class KafkaTopicConfig {
    /**
     * The main topic for order creation events.
     * 3 partitions allow parallel consumption by up to 3 Payment Service instances.
     */
    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_CREATED)
                .partitions(3)
                .replicas(1)  // Increase to 3 in production
                .build();
    }

    /**
     * Dead Letter Queue for failed order.created message processing.
     * Failed messages land here after exhausting retries.
     */
    @Bean
    public NewTopic orderCreatedDlqTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_CREATED_DLQ)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
