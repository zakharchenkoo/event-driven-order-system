package com.orderSystem.orderservice.repository;

import com.orderSystem.orderservice.entity.Order;
import com.orderSystem.orderservice.entity.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

/**
 * Spring Data JPA repository for {@link Order} entities.
 *
 * <p>Provides standard CRUD operations plus custom queries for
 * order lifecycle management.</p>
 */
@Repository
public interface OrderRepository extends JpaRepository<Order, UUID> {

    /**
     * Finds all orders belonging to a specific customer.
     *
     * @param customerId the ID of the customer
     * @return list of orders for the given customer, may be empty
     */
    List<Order> findByCustomerId(UUID customerId);

    /**
     * Finds all orders with a specific lifecycle status.
     *
     * @param status the order status to filter by
     * @return list of matching orders
     */
    List<Order> findByStatus(OrderStatus status);
}
