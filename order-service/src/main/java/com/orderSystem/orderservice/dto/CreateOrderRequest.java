package com.orderSystem.orderservice.dto;

import jakarta.validation.constraints.*;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Data Transfer Object for the create-order REST endpoint.
 *
 * <p>Validated via Bean Validation annotations before reaching the service layer.</p>
 */
public class CreateOrderRequest {
    @NotNull(message = "Customer ID is required")
    private UUID customerId;

    @NotNull(message = "Total amount is required")
    @DecimalMin(value = "0.01", message = "Total amount must be greater than zero")
    @Digits(integer = 15, fraction = 4, message = "Invalid amount format")
    private BigDecimal totalAmount;

    @NotBlank(message = "Currency is required")
    @Size(min = 3, max = 3, message = "Currency must be a 3-letter ISO-4217 code")
    private String currency;

    // -------------------------------------------------------------------------
    // Getters & Setters
    // -------------------------------------------------------------------------

    public UUID getCustomerId() { return customerId; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }

    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }
}
