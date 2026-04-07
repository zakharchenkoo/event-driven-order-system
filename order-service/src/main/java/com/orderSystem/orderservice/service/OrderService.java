package com.orderSystem.orderservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderSystem.orderservice.dto.CreateOrderRequest;
import com.orderSystem.orderservice.dto.OrderResponse;
import com.orderSystem.orderservice.entity.Order;
import com.orderSystem.orderservice.entity.OrderStatus;
import com.orderSystem.orderservice.entity.OutboxEvent;
import com.orderSystem.orderservice.exception.OrderNotFoundException;
import com.orderSystem.orderservice.repository.OrderRepository;
import com.orderSystem.orderservice.repository.OutboxEventRepository;
import com.orderSystem.shared.config.KafkaTopics;
import com.orderSystem.shared.events.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.orderSystem.shared.events.PaymentCompletedEvent;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Core business logic for the Order Service.
 *
 * <p>Handles order creation using the <b>Transactional Outbox Pattern</b>:
 * the order record and the outbox event are persisted atomically in a single
 * database transaction. A separate scheduler ({@link OutboxRelayService})
 * publishes the outbox entries to Kafka asynchronously.</p>
 *
 * <p>This approach guarantees that an {@code OrderCreatedEvent} is eventually
 * published even if the application crashes immediately after writing to the DB.</p>
 */
@Service
public class OrderService {
    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxEventRepository,
                        ObjectMapper objectMapper) {
        this.orderRepository = orderRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    // -------------------------------------------------------------------------
    // Create Order
    // -------------------------------------------------------------------------

    /**
     * Creates a new order and schedules an {@link OrderCreatedEvent} for Kafka publication
     * via the Transactional Outbox Pattern.
     *
     * <p>Both the {@link Order} and the {@link OutboxEvent} are persisted
     * within a single ACID transaction.</p>
     *
     * @param request the validated create-order request
     * @return the created order as a response DTO
     * @throws JsonProcessingException if event serialization fails (runtime — should not happen)
     */
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        log.info("Creating order for customer: {}", request.getCustomerId());

        // 1. Persist the order entity
        Order order = new Order();
        order.setCustomerId(request.getCustomerId());
        order.setTotalAmount(request.getTotalAmount());
        order.setCurrency(request.getCurrency());
        order.setStatus(OrderStatus.PENDING);
        Order savedOrder = orderRepository.save(order);

        log.info("Order persisted with ID: {}", savedOrder.getId());

        // 2. Build the Kafka event
        OrderCreatedEvent event = new OrderCreatedEvent(
                savedOrder.getId(),
                savedOrder.getCustomerId(),
                savedOrder.getTotalAmount(),
                savedOrder.getCurrency()
        );

        // 3. Write the event to the outbox table (same transaction)
        try {
            OutboxEvent outboxEvent = new OutboxEvent();
            outboxEvent.setTopic(KafkaTopics.ORDER_CREATED);
            outboxEvent.setAggregateType("Order");
            outboxEvent.setAggregateId(savedOrder.getId());
            outboxEvent.setPayload(objectMapper.writeValueAsString(event));
            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException e) {
            // Serialization failure — roll back the transaction
            throw new RuntimeException("Failed to serialize OrderCreatedEvent", e);
        }

        log.info("Outbox event written for order: {}", savedOrder.getId());
        return OrderResponse.from(savedOrder);
    }

    // -------------------------------------------------------------------------
    // Queries
    // -------------------------------------------------------------------------

    /**
     * Retrieves a single order by its ID.
     *
     * @param orderId the order's unique ID
     * @return the order response DTO
     * @throws OrderNotFoundException if no order exists with the given ID
     */
    @Transactional(readOnly = true)
    public OrderResponse getOrderById(UUID orderId) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));
        return OrderResponse.from(order);
    }

    /**
     * Retrieves all orders for a given customer.
     *
     * @param customerId the customer's unique ID
     * @return list of order response DTOs, may be empty
     */
    @Transactional(readOnly = true)
    public List<OrderResponse> getOrdersByCustomer(UUID customerId) {
        return orderRepository.findByCustomerId(customerId)
                .stream()
                .map(OrderResponse::from)
                .collect(Collectors.toList());
    }

    /**
     * Updates the status of an existing order.
     *
     * @param orderId   the ID of the order to update
     * @param newStatus the new order status
     * @return the updated order response DTO
     * @throws OrderNotFoundException if the order doesn't exist
     */
    @Transactional
    public OrderResponse updateOrderStatus(UUID orderId, OrderStatus newStatus) {
        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + orderId));

        log.info("Updating order {} status from {} to {}", orderId, order.getStatus(), newStatus);
        order.setStatus(newStatus);
        return OrderResponse.from(orderRepository.save(order));
    }

    /**
     * Updates order status based on payment result.
     * SUCCESS → CONFIRMED, FAILED → CANCELLED.
     *
     * @param event the payment completed event received from Kafka
     */
    @Transactional
    public void handlePaymentCompleted(PaymentCompletedEvent event) {
        UUID orderId = event.getOrderId();

        Order order = orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(
                        "Order not found: " + orderId));

        OrderStatus newStatus = switch (event.getPaymentStatus()) {
            case "SUCCESS" -> OrderStatus.CONFIRMED;
            case "FAILED"  -> OrderStatus.CANCELLED;
            default -> {
                log.warn("Unknown payment status {} for order {}",
                        event.getPaymentStatus(), orderId);
                yield order.getStatus();
            }
        };

        order.setStatus(newStatus);
        orderRepository.save(order);

        log.info("Order {} status updated to {}", orderId, newStatus);
    }
}
