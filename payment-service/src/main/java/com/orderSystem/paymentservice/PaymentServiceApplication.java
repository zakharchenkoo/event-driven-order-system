package com.orderSystem.paymentservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Payment Service microservice.
 *
 * <p>This service:
 * <ul>
 *     <li>Consumes {@code OrderCreatedEvent} from the {@code order.created} Kafka topic</li>
 *     <li>Processes payment with idempotency guarantees</li>
 *     <li>Publishes {@code PaymentCompletedEvent} to the {@code payment.completed} topic</li>
 * </ul>
 * </p>
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
