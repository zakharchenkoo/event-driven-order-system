package com.orderSystem.orderservice.entity;

/**
 * Represents the possible lifecycle states of an {@link Order}.
 *
 * <pre>
 * State machine:
 *   PENDING ──► CONFIRMED ──► PAID
 *      └──────────────────────────► CANCELLED
 * </pre>
 */
public enum OrderStatus {
    /** Order has been received but not yet validated or paid. */
    PENDING,

    /** Order has been validated and is awaiting payment processing. */
    CONFIRMED,

    /** Payment has been successfully processed for this order. */
    PAID,

    /** Order was cancelled before payment could be completed. */
    CANCELLED
}
