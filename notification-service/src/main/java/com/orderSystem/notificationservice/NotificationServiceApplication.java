package com.orderSystem.notificationservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Notification Service microservice.
 *
 * <p>This service:
 * <ul>
 *     <li>Consumes {@code PaymentCompletedEvent} from the {@code payment.completed} Kafka topic</li>
 *     <li>Dispatches customer notifications (email simulation via logging)</li>
 * </ul>
 * </p>
 *
 * <p>This service has no database — it is a pure event consumer with no
 * stateful persistence requirements in this implementation.</p>
 */
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
