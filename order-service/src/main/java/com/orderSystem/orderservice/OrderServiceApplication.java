package com.orderSystem.orderservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for the Order Service microservice.
 *
 * <p>{@link EnableScheduling} activates the outbox relay scheduler
 * ({@link com.orderSystem.orderservice.service.OutboxRelayService})
 * that periodically publishes pending outbox events to Kafka.</p>
 */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
