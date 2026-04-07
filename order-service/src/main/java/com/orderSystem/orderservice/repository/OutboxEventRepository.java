package com.orderSystem.orderservice.repository;

import com.orderSystem.orderservice.entity.OutboxEvent;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * Spring Data JPA repository for {@link OutboxEvent} entities.
 *
 * <p>Used by the outbox relay scheduler to find and publish pending events.</p>
 */
@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    /**
     * Finds up to 10 unprocessed outbox events using a pessimistic lock.
     *
     * <p>{@code FOR UPDATE} locks the selected rows so no other transaction
     * can modify them until this transaction completes.</p>
     *
     * <p>{@code SKIP LOCKED} means competing instances skip already-locked rows
     * instead of waiting — enabling true parallel processing across instances.</p>
     *
     * @return list of up to 10 unprocessed outbox entries, ordered by creation time
     */
    @Query(value = """
            SELECT * FROM outbox_events
            WHERE processed = false
            ORDER BY created_at ASC
            LIMIT 10
            FOR UPDATE SKIP LOCKED
            """, nativeQuery = true)
    List<OutboxEvent> findPendingEventsWithLock();
}
