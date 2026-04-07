package com.orderSystem.orderservice.entity;

import jakarta.persistence.*;

import java.time.Instant;
import java.util.UUID;

/**
 * Entity representing a pending outbox message for the Transactional Outbox Pattern.
 *
 * <p>The Outbox Pattern guarantees at-least-once event delivery by writing the event
 * to the same database transaction as the business data. A separate relay process
 * (or Spring scheduler) reads unprocessed outbox entries and publishes them to Kafka.</p>
 *
 * <p>Benefits:
 * <ul>
 *     <li>Prevents dual-write failures (DB write succeeds but Kafka publish fails).</li>
 *     <li>Ensures events are published even after application crashes.</li>
 * </ul>
 * </p>
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    /** The Kafka topic this event should be published to. */
    @Column(name = "topic", nullable = false)
    private String topic;

    /** The aggregate type that generated this event (e.g., "Order"). */
    @Column(name = "aggregate_type", nullable = false)
    private String aggregateType;

    /** The ID of the aggregate root (e.g., order ID). */
    @Column(name = "aggregate_id", nullable = false)
    private UUID aggregateId;

    /** JSON-serialized event payload to be published. */
    @Column(name = "payload", nullable = false, columnDefinition = "TEXT")
    private String payload;

    /** UTC timestamp of when this outbox entry was created. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** UTC timestamp of when this event was successfully published. Null if not yet processed. */
    @Column(name = "processed_at")
    private Instant processedAt;

    /** Whether this outbox entry has been successfully published to Kafka. */
    @Column(name = "processed", nullable = false)
    private boolean processed = false;

    // -------------------------------------------------------------------------
    // Lifecycle callbacks
    // -------------------------------------------------------------------------

    @PrePersist
    protected void onCreate() {
        this.createdAt = Instant.now();
    }

    // -------------------------------------------------------------------------
    // Getters & Setters
    // -------------------------------------------------------------------------

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public String getAggregateType() { return aggregateType; }
    public void setAggregateType(String aggregateType) { this.aggregateType = aggregateType; }

    public UUID getAggregateId() { return aggregateId; }
    public void setAggregateId(UUID aggregateId) { this.aggregateId = aggregateId; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public Instant getProcessedAt() { return processedAt; }
    public void setProcessedAt(Instant processedAt) { this.processedAt = processedAt; }

    public boolean isProcessed() { return processed; }
    public void setProcessed(boolean processed) { this.processed = processed; }
}
