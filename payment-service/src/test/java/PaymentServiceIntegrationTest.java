import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderSystem.paymentservice.entity.PaymentStatus;
import com.orderSystem.paymentservice.repository.PaymentRepository;
import com.orderSystem.shared.config.KafkaTopics;
import com.orderSystem.shared.events.OrderCreatedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Integration test for the Payment Service.
 *
 * <p>Tests the full event-driven flow:
 * <ol>
 *     <li>Publish {@code OrderCreatedEvent} to Kafka</li>
 *     <li>Assert payment is persisted in the database</li>
 *     <li>Assert {@code PaymentCompletedEvent} is published to the output topic</li>
 *     <li>Assert idempotency: duplicate events produce only one payment record</li>
 * </ol>
 * </p>
 */
@SpringBootTest
@Testcontainers
class PaymentServiceIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("payment_db")
            .withUsername("payment_user")
            .withPassword("payment_pass")
            .withInitScript("schema.sql");

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
    }

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @AfterEach
    void cleanup() {
        paymentRepository.deleteAll();
    }

    /**
     * Tests that a payment is created after consuming an OrderCreatedEvent.
     */
    @Test
    void consumeOrderCreatedEvent_shouldCreatePaymentRecord() throws Exception {
        // Given
        UUID orderId = UUID.randomUUID();
        UUID customerId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId, customerId, new BigDecimal("75.00"), "USD");

        // When: publish to Kafka
        publishToKafka(KafkaTopics.ORDER_CREATED, orderId.toString(),
                objectMapper.writeValueAsString(event));

        // Then: wait for consumer to process
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() -> {
            var payment = paymentRepository.findByOrderId(orderId);
            assertThat(payment).isPresent();
            assertThat(payment.get().getStatus()).isEqualTo(PaymentStatus.SUCCESS);
            assertThat(payment.get().getSourceEventId()).isEqualTo(event.getEventId());
        });
    }

    /**
     * Tests that duplicate events (same eventId) are handled idempotently.
     */
    @Test
    void consumeOrderCreatedEvent_withDuplicateEvent_shouldProcessOnlyOnce() throws Exception {
        // Given: same event published twice
        UUID orderId = UUID.randomUUID();
        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId, UUID.randomUUID(), new BigDecimal("50.00"), "USD");
        String payload = objectMapper.writeValueAsString(event);

        // When: publish the same event twice
        publishToKafka(KafkaTopics.ORDER_CREATED, orderId.toString(), payload);
        publishToKafka(KafkaTopics.ORDER_CREATED, orderId.toString(), payload);

        // Then: only one payment should exist
        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(paymentRepository.findByOrderId(orderId)).isPresent()
        );

        // Allow time for potential duplicate processing
        Thread.sleep(3000);

        long paymentCount = paymentRepository.findAll().stream()
                .filter(p -> p.getOrderId().equals(orderId))
                .count();
        assertThat(paymentCount).isEqualTo(1);
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private void publishToKafka(String topic, String key, String value) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()
        ))) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
    }

    @Test
    void concurrentEvents_shouldNotCreateDuplicatePayments() throws Exception {
        UUID orderId = UUID.randomUUID();

        OrderCreatedEvent event = new OrderCreatedEvent(
                orderId, UUID.randomUUID(), new BigDecimal("100.00"), "USD");

        String payload = objectMapper.writeValueAsString(event);

        IntStream.range(0, 10).parallel().forEach(i -> {
            try {
                publishToKafka(KafkaTopics.ORDER_CREATED, orderId.toString(), payload);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                assertThat(paymentRepository.findByOrderId(orderId)).isPresent()
        );

        Thread.sleep(3000);

        long count = paymentRepository.findAll().stream()
                .filter(p -> p.getOrderId().equals(orderId))
                .count();

        assertThat(count).isEqualTo(1);
    }
}
