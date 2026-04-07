package com.orderSystem.shared.config;

/**
 * Centralized registry of all Kafka topic names used across the event-driven order system.
 *
 * <p>Defining topic names as constants in the shared module prevents typos and
 * ensures all services refer to the same topic strings without duplication.</p>
 *
 * <p>Topic naming convention: {@code <domain>.<event-verb>}</p>
 */

public final class KafkaTopics {
    private KafkaTopics() {
        // Utility class — not instantiable
    }

    /**
     * Topic for order creation events.
     * Producer: Order Service
     * Consumer: Payment Service
     */
    public static final String ORDER_CREATED = "order.created";

    /**
     * Topic for payment completion events.
     * Producer: Payment Service
     * Consumer: Notification Service
     */
    public static final String PAYMENT_COMPLETED = "payment.completed";

    /**
     * Dead Letter Queue topic for failed {@code order.created} event processing.
     * Messages are routed here after exhausting retry attempts.
     */
    public static final String ORDER_CREATED_DLQ = "order.created.dlq";

    /**
     * Dead Letter Queue topic for failed {@code payment.completed} event processing.
     */
    public static final String PAYMENT_COMPLETED_DLQ = "payment.completed.dlq";
}
