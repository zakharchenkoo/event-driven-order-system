package com.orderSystem.orderservice.controller;

import com.orderSystem.orderservice.dto.CreateOrderRequest;
import com.orderSystem.orderservice.dto.OrderResponse;
import com.orderSystem.orderservice.entity.OrderStatus;
import com.orderSystem.orderservice.service.OrderService;
import jakarta.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

/**
 * REST controller exposing the Order Service API.
 *
 * <p>Base path: {@code /api/v1/orders}</p>
 *
 * <p>Endpoints:
 * <ul>
 *     <li>{@code POST /api/v1/orders} — Create a new order</li>
 *     <li>{@code GET /api/v1/orders/{id}} — Get a single order by ID</li>
 *     <li>{@code GET /api/v1/orders?customerId=...} — List orders by customer</li>
 *     <li>{@code PATCH /api/v1/orders/{id}/status} — Update order status</li>
 * </ul>
 * </p>
 */
@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {
    private static final Logger log = LoggerFactory.getLogger(OrderController.class);

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Creates a new order.
     *
     * <p>On success, the order is persisted and an {@code OrderCreatedEvent} is
     * scheduled for Kafka publication via the Outbox Pattern.</p>
     *
     * @param request the validated create-order request body
     * @return {@code 201 Created} with the new order details
     */
    @PostMapping
    public ResponseEntity<OrderResponse> createOrder(@Valid @RequestBody CreateOrderRequest request) {
        log.info("Received create order request for customer: {}", request.getCustomerId());
        OrderResponse response = orderService.createOrder(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * Retrieves a single order by its unique ID.
     *
     * @param orderId the order's UUID
     * @return {@code 200 OK} with the order, or {@code 404 Not Found}
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderResponse> getOrder(@PathVariable UUID orderId) {
        log.info("Fetching order: {}", orderId);
        return ResponseEntity.ok(orderService.getOrderById(orderId));
    }

    /**
     * Lists all orders placed by a specific customer.
     *
     * @param customerId the customer's UUID
     * @return {@code 200 OK} with list of orders (may be empty)
     */
    @GetMapping
    public ResponseEntity<List<OrderResponse>> getOrdersByCustomer(
            @RequestParam UUID customerId) {
        log.info("Fetching orders for customer: {}", customerId);
        return ResponseEntity.ok(orderService.getOrdersByCustomer(customerId));
    }

    /**
     * Updates the status of an existing order.
     *
     * @param orderId   the order's UUID
     * @param newStatus the new status value
     * @return {@code 200 OK} with the updated order
     */
    @PatchMapping("/{orderId}/status")
    public ResponseEntity<OrderResponse> updateStatus(
            @PathVariable UUID orderId,
            @RequestParam OrderStatus newStatus) {
        log.info("Updating order {} status to {}", orderId, newStatus);
        return ResponseEntity.ok(orderService.updateOrderStatus(orderId, newStatus));
    }
}
