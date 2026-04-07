package com.orderSystem.shared.events;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Event published by the Order Service when a new order is successfully created.
 *
 * <p>This event is consumed by the Payment Service to trigger payment processing.
 * It is serialized as JSON and transmitted over the {@code order.created} Kafka topic.</p>
 *
 * <p>Design notes:
 * <ul>
 *     <li>Immutable value object — all fields set at construction time.</li>
 *     <li>Contains a unique {@code eventId} for idempotent consumer processing.</li>
 *     <li>Uses {@link Instant} for timezone-safe timestamp handling.</li>
 * </ul>
 * </p>
 */

public class OrderCreatedEvent {

    /** Unique identifier for this event instance (used for idempotency checks). */
    private UUID eventId;

    /** The ID of the order that was created. */
    private UUID orderId;

    /** The ID of the customer who placed the order. */
    private UUID customerId;

    /** Total monetary amount of the order. */
    private BigDecimal totalAmount;

    /** ISO-4217 currency code (e.g., "USD"). */
    private String currency;

    /** UTC timestamp indicating when this event was created. */
    @JsonFormat(shape = JsonFormat.Shape.STRING)
    private Instant occurredAt;

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /** No-arg constructor required for Jackson deserialization. */
    public OrderCreatedEvent() {}

    /**
     * Full constructor for creating an {@code OrderCreatedEvent}.
     *
     * @param orderId     the unique ID of the created order
     * @param customerId  the unique ID of the customer
     * @param totalAmount the total amount due for the order
     * @param currency    the currency code (e.g., "USD")
     */
    public OrderCreatedEvent(UUID orderId, UUID customerId, BigDecimal totalAmount, String currency) {
        this.eventId = UUID.randomUUID();
        this.orderId = orderId;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.currency = currency;
        this.occurredAt = Instant.now();
    }

    // -------------------------------------------------------------------------
    // Getters & Setters
    // -------------------------------------------------------------------------

    public UUID getEventId() { return eventId; }
    public void setEventId(UUID eventId) { this.eventId = eventId; }

    public UUID getOrderId() { return orderId; }
    public void setOrderId(UUID orderId) { this.orderId = orderId; }

    public UUID getCustomerId() { return customerId; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }

    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }

    @Override
    public String toString() {
        return "OrderCreatedEvent{" +
                "eventId=" + eventId +
                ", orderId=" + orderId +
                ", customerId=" + customerId +
                ", totalAmount=" + totalAmount +
                ", currency='" + currency + '\'' +
                ", occurredAt=" + occurredAt +
                '}';
    }
}
