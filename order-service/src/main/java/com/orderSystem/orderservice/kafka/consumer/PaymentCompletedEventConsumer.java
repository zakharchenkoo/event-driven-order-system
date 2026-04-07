package com.orderSystem.orderservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderSystem.orderservice.service.OrderService;
import com.orderSystem.shared.events.PaymentCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that listens to payment.Completed topic and updates
 * the order status based on the payment result.
 *
 * <p>Closes the feedback loop: Order → Payment → Order status update</p>
 *
 * <p>Uses the same consumer group as the rest of order-service so all
 * 3 instances share the load across partitions (one partition per instance).</p>
 */
@Component
public class PaymentCompletedEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedEventConsumer.class);

    private final OrderService orderService;
    private final ObjectMapper objectMapper;

    public PaymentCompletedEventConsumer(OrderService orderService, ObjectMapper objectMapper) {
        this.orderService = orderService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            topics = "payment.completed",
            groupId = "order-service-payment-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onPaymentCompleted(String message, Acknowledgment acknowledgment) {
        try {
            PaymentCompletedEvent event = objectMapper.readValue(message, PaymentCompletedEvent.class);

            log.info("Received PaymentCompletedEvent | orderId: {} | status: {}",
                    event.getOrderId(), event.getPaymentStatus());

            orderService.handlePaymentCompleted(event);

            acknowledgment.acknowledge();
            log.info("PaymentCompletedEvent acknowledged | orderId: {}", event.getOrderId());

        } catch (Exception e) {
            log.error("Failed to process PaymentCompletedEvent: {}", e.getMessage(), e);
        }
    }
}
