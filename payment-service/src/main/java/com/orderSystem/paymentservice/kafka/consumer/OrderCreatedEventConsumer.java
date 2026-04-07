package com.orderSystem.paymentservice.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderSystem.paymentservice.service.PaymentService;
import com.orderSystem.shared.config.KafkaTopics;
import com.orderSystem.shared.events.OrderCreatedEvent;
import com.orderSystem.shared.events.PaymentCompletedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that listens for {@link OrderCreatedEvent} messages on the
 * {@code order.created} topic.
 *
 * <p>On receipt of an event:
 * <ol>
 *     <li>Deserializes the JSON payload to an {@link OrderCreatedEvent}</li>
 *     <li>Delegates to {@link PaymentService} for processing</li>
 *     <li>Publishes the resulting {@link PaymentCompletedEvent} to Kafka</li>
 *     <li>Manually acknowledges the message to Kafka (MANUAL_IMMEDIATE ack mode)</li>
 * </ol>
 * </p>
 *
 * <p><b>Error handling:</b> If processing fails, the message is NOT acknowledged
 * and Spring Kafka's retry/recovery mechanism takes over. After exhausting retries,
 * the message is sent to the Dead Letter Queue ({@code order.created.dlq}).</p>
 */
@Component
public class OrderCreatedEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderCreatedEventConsumer.class);

    private final PaymentService paymentService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public OrderCreatedEventConsumer(PaymentService paymentService,
                                     KafkaTemplate<String, String> kafkaTemplate,
                                     ObjectMapper objectMapper) {
        this.paymentService = paymentService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Listens for {@link OrderCreatedEvent} messages on the {@code order.created} topic.
     *
     * <p>Uses {@code MANUAL_IMMEDIATE} acknowledgement mode to give the application
     * full control over when the offset is committed.</p>
     *
     * @param record the raw Kafka consumer record
     * @param acknowledgment the Kafka acknowledgement handle
     */
    @KafkaListener(
            topics = KafkaTopics.ORDER_CREATED,
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handleOrderCreated(ConsumerRecord<String, String> record,
                                   Acknowledgment acknowledgment) {
        log.info("Received OrderCreatedEvent | partition: {} | offset: {} | key: {}",
                record.partition(), record.offset(), record.key());

        try {
            // 1. Deserialize event
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
            log.info("Processing OrderCreatedEvent for orderId: {}", event.getOrderId());

            // 2. Process payment — returns PaymentCompletedEvent
            PaymentCompletedEvent paymentCompletedEvent = paymentService.processPayment(event);

            // 3. Publish result to Kafka
            String payload = objectMapper.writeValueAsString(paymentCompletedEvent);
            kafkaTemplate.send(KafkaTopics.PAYMENT_COMPLETED,
                            paymentCompletedEvent.getOrderId().toString(),
                            payload)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish PaymentCompletedEvent for order {}: {}",
                                    paymentCompletedEvent.getOrderId(), ex.getMessage());
                        } else {
                            log.info("PaymentCompletedEvent published for order: {} | status: {}",
                                    paymentCompletedEvent.getOrderId(),
                                    paymentCompletedEvent.getPaymentStatus());
                        }
                    });

            // 4. Acknowledge successful processing
            acknowledgment.acknowledge();
            log.info("OrderCreatedEvent acknowledged for orderId: {}", event.getOrderId());

        } catch (Exception e) {
            log.error("Failed to process OrderCreatedEvent from partition {} offset {}: {}",
                    record.partition(), record.offset(), e.getMessage(), e);
            // Do NOT acknowledge — Spring Kafka retry/DLQ mechanism handles this
            throw new RuntimeException("Failed to process OrderCreatedEvent", e);
        }
    }
}
