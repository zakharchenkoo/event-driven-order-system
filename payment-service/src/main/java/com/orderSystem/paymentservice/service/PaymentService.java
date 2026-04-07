package com.orderSystem.paymentservice.service;

import com.orderSystem.paymentservice.entity.Payment;
import com.orderSystem.paymentservice.entity.PaymentStatus;
import com.orderSystem.paymentservice.repository.PaymentRepository;
import com.orderSystem.shared.events.OrderCreatedEvent;
import com.orderSystem.shared.events.PaymentCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Core business logic for the Payment Service.
 *
 * <p>Processes incoming {@link OrderCreatedEvent} messages by:
 * <ol>
 *     <li>Performing an <b>idempotency check</b> using the event's {@code eventId}</li>
 *     <li>Simulating payment gateway processing</li>
 *     <li>Persisting the payment record</li>
 *     <li>Returning a {@link PaymentCompletedEvent} for Kafka publication</li>
 * </ol>
 * </p>
 *
 * <p><b>Idempotency guarantee</b>: if the same event is received more than once
 * (e.g., due to Kafka redelivery after a consumer crash), the second call is a
 * no-op — the existing payment record is returned without double-charging.</p>
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;

    public PaymentService(PaymentRepository paymentRepository) {
        this.paymentRepository = paymentRepository;
    }

    /**
     * Processes a payment for the given order creation event.
     *
     * @param event the order creation event containing payment details
     * @return a {@link PaymentCompletedEvent} representing the payment outcome
     */
    @Transactional
    public PaymentCompletedEvent processPayment(OrderCreatedEvent event) {
        log.info("Processing payment for order: {} | eventId: {}", event.getOrderId(), event.getEventId());

        // ── Idempotency check ──────────────────────────────────────────────────
        // If we've already processed this event, return the previously recorded outcome.
        var existingPayment = paymentRepository.findBySourceEventId(event.getEventId());
        if (existingPayment.isPresent()) {
            log.warn("Duplicate event detected for eventId: {}. Returning cached result.", event.getEventId());
            return buildPaymentCompletedEvent(existingPayment.get());
        }

        // ── Simulate payment gateway ───────────────────────────────────────────
        PaymentStatus paymentStatus = simulatePaymentGateway(event);

        // ── Persist payment record ─────────────────────────────────────────────
        Payment payment = new Payment();
        payment.setOrderId(event.getOrderId());
        payment.setCustomerId(event.getCustomerId());
        payment.setAmount(event.getTotalAmount());
        payment.setCurrency(event.getCurrency());
        payment.setStatus(paymentStatus);
        payment.setSourceEventId(event.getEventId());
        Payment savedPayment = paymentRepository.save(payment);

        log.info("Payment {} persisted with status: {} for order: {}",
                savedPayment.getId(), paymentStatus, event.getOrderId());

        return buildPaymentCompletedEvent(savedPayment);
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Simulates a payment gateway call.
     *
     * <p>In a real system, this would integrate with Stripe, PayPal, etc.
     * For demo purposes: amounts ending in .99 are simulated as failures.</p>
     *
     * @param event the order event
     * @return the simulated payment outcome
     */
    private PaymentStatus simulatePaymentGateway(OrderCreatedEvent event) {
        log.debug("Calling simulated payment gateway for order: {}", event.getOrderId());

        // Simulate ~10% failure rate for demo purposes
        if (event.getTotalAmount().scale() >= 2) {
            String amountStr = event.getTotalAmount().toPlainString();
            if (amountStr.endsWith(".99")) {
                log.warn("Simulated payment FAILURE for order: {} (amount ending in .99)", event.getOrderId());
                return PaymentStatus.FAILED;
            }
        }

        // Simulate processing latency (remove in production)
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return PaymentStatus.SUCCESS;
    }

    /**
     * Constructs a {@link PaymentCompletedEvent} from a persisted {@link Payment}.
     *
     * @param payment the persisted payment entity
     * @return the corresponding payment completed event
     */
    private PaymentCompletedEvent buildPaymentCompletedEvent(Payment payment) {
        return new PaymentCompletedEvent(
                payment.getId(),
                payment.getOrderId(),
                payment.getCustomerId(),
                payment.getAmount(),
                payment.getCurrency(),
                payment.getStatus().name()
        );
    }
}
