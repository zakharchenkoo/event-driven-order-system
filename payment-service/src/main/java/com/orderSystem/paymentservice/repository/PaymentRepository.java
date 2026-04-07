package com.orderSystem.paymentservice.repository;

import com.orderSystem.paymentservice.entity.Payment;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

/**
 * Spring Data JPA repository for {@link Payment} entities.
 */
@Repository
public interface PaymentRepository extends JpaRepository<Payment, UUID> {

    /**
     * Finds a payment by the source Kafka event ID.
     *
     * <p>Used for <b>idempotency checks</b>: if a payment already exists for a given
     * {@code eventId}, the consumer skips reprocessing to avoid duplicate charges.</p>
     *
     * @param sourceEventId the Kafka event ID from the {@code OrderCreatedEvent}
     * @return the existing payment if found
     */
    Optional<Payment> findBySourceEventId(UUID sourceEventId);

    /**
     * Finds the payment associated with a specific order.
     *
     * @param orderId the order ID to look up
     * @return the payment record if it exists
     */
    Optional<Payment> findByOrderId(UUID orderId);
}
