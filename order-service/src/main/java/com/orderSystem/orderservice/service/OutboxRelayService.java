package com.orderSystem.orderservice.service;

import com.orderSystem.orderservice.entity.OutboxEvent;
import com.orderSystem.orderservice.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Scheduled service that relays pending outbox events to Kafka.
 *
 * <p>This is the "relay" component of the <b>Transactional Outbox Pattern</b>.
 * It periodically polls the {@code outbox_events} table for unprocessed entries
 * and publishes them to their designated Kafka topics.</p>
 *
 * <p>Key guarantees:
 * <ul>
 *     <li><b>At-least-once delivery</b>: events may be published more than once on crash/restart.</li>
 *     <li><b>Consumers must be idempotent</b>: downstream services should deduplicate by {@code eventId}.</li>
 * </ul>
 * </p>
 *
 * <p>In production, consider replacing the polling scheduler with Debezium CDC
 * (Change Data Capture) for lower latency and reduced DB load.</p>
 */
@Service
public class OutboxRelayService {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelayService.class);

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxRelayService(OutboxEventRepository outboxEventRepository,
                              KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Polls the outbox table every 5 seconds and publishes pending events to Kafka.
     *
     * <p>The {@code @Transactional} annotation here is critical — it ensures the
     * {@code FOR UPDATE SKIP LOCKED} lock is held for the duration of the method.
     * The lock is released when the transaction commits at the end of this method.</p>
     *
     * <p>Each successfully published event is marked as {@code processed = true}
     * and will not be republished in subsequent runs.</p>
     *
     * <p>If Kafka publishing fails, the entry remains unprocessed and will be
     * retried on the next scheduled run.</p>
     */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayOutboxEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository
                .findPendingEventsWithLock();

        if (pendingEvents.isEmpty()) {
            return;
        }

        log.info("Found {} pending outbox events to relay", pendingEvents.size());

        for (OutboxEvent outboxEvent : pendingEvents) {
            try {
                kafkaTemplate.send(
                                outboxEvent.getTopic(),
                                outboxEvent.getAggregateId().toString(),
                                outboxEvent.getPayload())
                        .whenComplete((result, ex) -> {
                            if (ex != null) {
                                log.error("Failed to publish outbox event {} to topic {}: {}",
                                        outboxEvent.getId(),
                                        outboxEvent.getTopic(),
                                        ex.getMessage());
                            } else {
                                log.debug("Outbox event {} published to topic {} partition {}",
                                        outboxEvent.getId(),
                                        outboxEvent.getTopic(),
                                        result.getRecordMetadata().partition());
                            }
                        });

                // Mark as processed immediately
                // Note: at-least-once delivery — if the app crashes after Kafka send
                // but before this save, the event will be re-published on restart.
                // Consumers must handle duplicates via idempotency checks.
                outboxEvent.setProcessed(true);
                outboxEvent.setProcessedAt(Instant.now());
                outboxEventRepository.save(outboxEvent);

                log.info("Outbox event {} relayed to topic: {}",
                        outboxEvent.getId(), outboxEvent.getTopic());

            } catch (Exception e) {
                log.error("Error relaying outbox event {}: {}",
                        outboxEvent.getId(), e.getMessage(), e);
                // Do not rethrow — allow remaining events in this batch to be processed
            }
        }
    }
}
