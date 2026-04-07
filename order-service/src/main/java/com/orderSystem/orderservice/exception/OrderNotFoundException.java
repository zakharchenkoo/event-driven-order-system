package com.orderSystem.orderservice.exception;

/**
 * Thrown when an order with the requested ID cannot be found in the database.
 */
public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(String message) {
        super(message);
    }

    public OrderNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
