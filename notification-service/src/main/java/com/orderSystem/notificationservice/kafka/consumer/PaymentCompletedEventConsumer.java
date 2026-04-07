package com.orderSystem.notificationservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderSystem.notificationservice.service.NotificationService;
import com.orderSystem.shared.config.KafkaTopics;
import com.orderSystem.shared.events.PaymentCompletedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that listens for {@link PaymentCompletedEvent} messages on the
 * {@code payment.completed} topic.
 *
 * <p>Delegates to {@link NotificationService} to dispatch customer notifications.</p>
 *
 * <p><b>Idempotency note:</b> this consumer does not perform explicit idempotency
 * checks since notifications are idempotent by nature — sending the same email
 * twice is a minor inconvenience, not a business-critical issue. For stricter
 * deduplication, add a processed-events table keyed by {@code eventId}.</p>
 */
@Component
public class PaymentCompletedEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentCompletedEventConsumer.class);

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public PaymentCompletedEventConsumer(NotificationService notificationService,
                                         ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    /**
     * Listens for {@link PaymentCompletedEvent} messages on the {@code payment.completed} topic.
     *
     * @param record         the raw Kafka consumer record
     * @param acknowledgment the manual acknowledgement handle
     */
    @KafkaListener(
            topics = KafkaTopics.PAYMENT_COMPLETED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handlePaymentCompleted(ConsumerRecord<String, String> record,
                                       Acknowledgment acknowledgment) {
        log.info("Received PaymentCompletedEvent | partition: {} | offset: {} | key: {}",
                record.partition(), record.offset(), record.key());

        try {
            PaymentCompletedEvent event = objectMapper.readValue(record.value(), PaymentCompletedEvent.class);
            log.info("Processing PaymentCompletedEvent for orderId: {} | status: {}",
                    event.getOrderId(), event.getPaymentStatus());

            notificationService.handlePaymentCompleted(event);

            acknowledgment.acknowledge();
            log.info("PaymentCompletedEvent acknowledged for orderId: {}", event.getOrderId());

        } catch (Exception e) {
            log.error("Failed to process PaymentCompletedEvent from offset {}: {}",
                    record.offset(), e.getMessage(), e);
            throw new RuntimeException("Failed to process PaymentCompletedEvent", e);
        }
    }
}
