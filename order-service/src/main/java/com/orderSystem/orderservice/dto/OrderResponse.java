package com.orderSystem.orderservice.dto;

import com.orderSystem.orderservice.entity.Order;
import com.orderSystem.orderservice.entity.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Data Transfer Object returned by the Order Service REST API.
 *
 * <p>Decouples the internal {@link Order} entity from the API contract,
 * ensuring internal changes don't break API consumers.</p>
 */
public class OrderResponse {
    private UUID id;
    private UUID customerId;
    private BigDecimal totalAmount;
    private String currency;
    private OrderStatus status;
    private Instant createdAt;

    // -------------------------------------------------------------------------
    // Static factory from entity
    // -------------------------------------------------------------------------

    /**
     * Creates an {@code OrderResponse} from an {@link Order} entity.
     *
     * @param order the order entity to map
     * @return the response DTO
     */
    public static OrderResponse from(Order order) {
        OrderResponse response = new OrderResponse();
        response.setId(order.getId());
        response.setCustomerId(order.getCustomerId());
        response.setTotalAmount(order.getTotalAmount());
        response.setCurrency(order.getCurrency());
        response.setStatus(order.getStatus());
        response.setCreatedAt(order.getCreatedAt());
        return response;
    }

    // -------------------------------------------------------------------------
    // Getters & Setters
    // -------------------------------------------------------------------------

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public UUID getCustomerId() { return customerId; }
    public void setCustomerId(UUID customerId) { this.customerId = customerId; }

    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }

    public String getCurrency() { return currency; }
    public void setCurrency(String currency) { this.currency = currency; }

    public OrderStatus getStatus() { return status; }
    public void setStatus(OrderStatus status) { this.status = status; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }
}
