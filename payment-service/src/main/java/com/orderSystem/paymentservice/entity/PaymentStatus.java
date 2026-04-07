package com.orderSystem.paymentservice.entity;

/**
 * Represents the outcome of a payment processing attempt.
 */
public enum PaymentStatus {

    /** Payment was processed and charged successfully. */
    SUCCESS,

    /** Payment processing failed (e.g., insufficient funds, gateway error). */
    FAILED
}
