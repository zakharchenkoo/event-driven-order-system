package com.orderSystem.notificationservice.service;

import com.orderSystem.shared.events.PaymentCompletedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Notification Service business logic.
 *
 * <p>Processes {@link PaymentCompletedEvent} messages and dispatches
 * customer notifications. In this implementation notifications are logged
 * to simulate email/SMS dispatch — a real system would integrate with
 * services like SendGrid, Twilio, or AWS SNS.</p>
 *
 * <p>This service is stateless — it does not persist notification records.
 * In production, consider adding a notification log table to support
 * re-sending and delivery tracking.</p>
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    /**
     * Handles a payment completed event by dispatching the appropriate notification.
     *
     * <p>Routes to success or failure notification based on {@code paymentStatus}.</p>
     *
     * @param event the payment completed event to process
     */
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        log.info("Processing notification for order: {} | status: {} | eventId: {}",
                event.getOrderId(), event.getPaymentStatus(), event.getEventId());

        switch (event.getPaymentStatus()) {
            case "SUCCESS" -> sendPaymentSuccessNotification(event);
            case "FAILED"  -> sendPaymentFailureNotification(event);
            default -> log.warn("Unknown payment status '{}' for order: {}",
                    event.getPaymentStatus(), event.getOrderId());
        }
    }

    // -------------------------------------------------------------------------
    // Notification dispatchers
    // -------------------------------------------------------------------------

    /**
     * Sends a payment success notification to the customer.
     *
     * <p>In production this would call an email/SMS provider API.</p>
     *
     * @param event the successful payment event
     */
    private void sendPaymentSuccessNotification(PaymentCompletedEvent event) {
        // ── Simulated email body ──────────────────────────────────────────────
        String notificationMessage = String.format(
                """
                ╔══════════════════════════════════════╗
                  📧 EMAIL NOTIFICATION (SIMULATED)
                ╚══════════════════════════════════════╝
                To:      customer-%s@example.com
                Subject: Your order has been confirmed! 🎉
                
                Dear Customer,
                
                Great news! Your payment has been successfully processed.
                
                  Order ID:    %s
                  Payment ID:  %s
                  Amount:      %s %s
                  Status:      ✅ PAYMENT SUCCESSFUL
                  Timestamp:   %s
                
                Your order is now being prepared. You will receive a
                shipping confirmation once your order is dispatched.
                
                Thank you for your purchase!
                """,
                event.getCustomerId(),
                event.getOrderId(),
                event.getPaymentId(),
                event.getAmount(),
                event.getCurrency(),
                event.getOccurredAt()
        );

        log.info("SUCCESS NOTIFICATION SENT:\n{}", notificationMessage);
    }

    /**
     * Sends a payment failure notification to the customer.
     *
     * @param event the failed payment event
     */
    private void sendPaymentFailureNotification(PaymentCompletedEvent event) {
        String notificationMessage = String.format(
                """
                ╔══════════════════════════════════════╗
                  📧 EMAIL NOTIFICATION (SIMULATED)
                ╚══════════════════════════════════════╝
                To:      customer-%s@example.com
                Subject: Action required: Payment issue with your order
                
                Dear Customer,
                
                Unfortunately, we were unable to process your payment.
                
                  Order ID:    %s
                  Amount:      %s %s
                  Status:      ❌ PAYMENT FAILED
                  Timestamp:   %s
                
                Please update your payment method and try again.
                If you continue to experience issues, contact our support team.
                """,
                event.getCustomerId(),
                event.getOrderId(),
                event.getAmount(),
                event.getCurrency(),
                event.getOccurredAt()
        );

        log.warn("FAILURE NOTIFICATION SENT:\n{}", notificationMessage);
    }
}
