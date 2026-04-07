package com.orderSystem.shared.events;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Event published by the Payment Service after a payment has been successfully processed.
 *
 * <p>This event is consumed by the Notification Service to trigger customer notifications.
 * It is serialized as JSON and transmitted over the {@code payment.completed} Kafka topic.</p>
 *
 * <p>Design notes:
 * <ul>
 *     <li>Includes {@code paymentStatus} to support future extensibility (e.g., FAILED events).</li>
 *     <li>Contains a unique {@code eventId} for idempotent consumer processing.</li>
 * </ul>
 * </p>
 */

public class PaymentCompletedEvent {

    /** Unique identifier for this event instance (used for idempotency checks). */
    private UUID eventId;

    /** The ID of the payment transaction. */
    private UUID paymentId;

    /** The ID of the order associated with this payment. */
    private UUID orderId;

    /** The ID of the customer who made the payment. */
    private UUID customerId;

    /** The amount that was charged. */
    private BigDecimal amount;

    /** ISO-4217 currency code (e.g., "USD"). */
    private String currency;

    /**
     * Status of the payment outcome.
     * Possible values: {@code SUCCESS}, {@code FAILED}.
     */
    private String paymentStatus;

    /** UTC timestamp indicating when this event was created. */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required for Jackson deserialization. */
    public PaymentCompletedEvent() {}

    /**
     * Full constructor for creating a {@code PaymentCompletedEvent}.
     *
     * @param paymentId     the unique ID of the payment record
     * @param orderId       the ID of the related order
     * @param customerId    the ID of the customer
     * @param amount        the amount charged
     * @param currency      the currency code (e.g., "USD")
     * @param paymentStatus the result of the payment (SUCCESS or FAILED)
     */
    public PaymentCompletedEvent(UUID paymentId, UUID orderId, UUID customerId,
                                 BigDecimal amount, String currency, String paymentStatus) {
        this.eventId = UUID.randomUUID();
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.customerId = customerId;
        this.amount = amount;
        this.currency = currency;
        this.paymentStatus = paymentStatus;
        this.occurredAt = Instant.now();
    }

    // -------------------------------------------------------------------------
    // Getters & Setters
    // -------------------------------------------------------------------------

    public UUID getEventId() { return eventId; }
    public void setEventId(UUID eventId) { this.eventId = eventId; }

    public UUID getPaymentId() { return paymentId; }
    public void setPaymentId(UUID paymentId) { this.paymentId = paymentId; }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public UUID getCustomerId() { return customerId; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }

    public BigDecimal getAmount() { return amount; }
    public void setAmount(BigDecimal amount) { this.amount = amount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public String getPaymentStatus() { return paymentStatus; }
    public void setPaymentStatus(String paymentStatus) { this.paymentStatus = paymentStatus; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    @Override
    public String toString() {
        return "PaymentCompletedEvent{" +
                "eventId=" + eventId +
                ", paymentId=" + paymentId +
                ", orderId=" + orderId +
                ", customerId=" + customerId +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", paymentStatus='" + paymentStatus + '\'' +
                ", occurredAt=" + occurredAt +
                '}';
    }
}
