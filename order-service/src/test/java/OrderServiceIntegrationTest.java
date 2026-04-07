import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderSystem.orderservice.dto.CreateOrderRequest;
import com.orderSystem.orderservice.entity.OrderStatus;
import com.orderSystem.orderservice.repository.OrderRepository;
import com.orderSystem.shared.config.KafkaTopics;
import com.orderSystem.shared.events.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * Integration test for the Order Service using Testcontainers.
 *
 * <p>Spins up real PostgreSQL and Kafka containers to verify the full
 * order creation flow:
 * <ol>
 *     <li>REST POST creates an order in the database</li>
 *     <li>An outbox entry is created in the same transaction</li>
 *     <li>The outbox relay publishes an {@code OrderCreatedEvent} to Kafka</li>
 *     <li>The Kafka message is verifiable via a test consumer</li>
 * </ol>
 * </p>
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
@Testcontainers
class OrderServiceIntegrationTest {

    // ── Testcontainers ─────────────────────────────────────────────────────────

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("order_db")
            .withUsername("order_user")
            .withPassword("order_pass")
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
        // Use validate so schema.sql is used, not auto DDL
        registry.add("spring.jpa.hibernate.ddl-auto", () -> "none");
    }

    // ── Autowired beans ────────────────────────────────────────────────────────

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private OrderRepository orderRepository;

    @AfterEach
    void cleanup() {
        orderRepository.deleteAll();
    }

    // ── Tests ──────────────────────────────────────────────────────────────────

    /**
     * Tests that creating an order via REST returns HTTP 201 with correct body.
     */
    @Test
    void createOrder_shouldReturn201WithOrderDetails() throws Exception {
        // Given
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId(UUID.randomUUID());
        request.setTotalAmount(new BigDecimal("99.99"));
        request.setCurrency("USD");

        // When & Then
        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isNotEmpty())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.currency").value("USD"))
                .andExpect(jsonPath("$.totalAmount").value(99.99));
    }

    /**
     * Tests that an order is persisted in the database after creation.
     */
    @Test
    void createOrder_shouldPersistOrderInDatabase() throws Exception {
        // Given
        UUID customerId = UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId(customerId);
        request.setTotalAmount(new BigDecimal("49.50"));
        request.setCurrency("EUR");

        // When
        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated());

        // Then
        var orders = orderRepository.findByCustomerId(customerId);
        assertThat(orders).hasSize(1);
        assertThat(orders.get(0).getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(orders.get(0).getTotalAmount()).isEqualByComparingTo("49.50");
    }

    /**
     * Tests that creating an order with invalid data returns HTTP 400.
     */
    @Test
    void createOrder_withInvalidData_shouldReturn400() throws Exception {
        // Given: negative amount (invalid)
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId(UUID.randomUUID());
        request.setTotalAmount(new BigDecimal("-10.00"));
        request.setCurrency("USD");

        // When & Then
        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isBadRequest());
    }

    /**
     * Tests that an OrderCreatedEvent is published to Kafka after order creation.
     * Uses a real Kafka consumer to verify the message.
     */
    @Test
    void createOrder_shouldPublishOrderCreatedEventToKafka() throws Exception {
        // Given
        UUID customerId = UUID.randomUUID();
        CreateOrderRequest request = new CreateOrderRequest();
        request.setCustomerId(customerId);
        request.setTotalAmount(new BigDecimal("150.00"));
        request.setCurrency("USD");

        // When: create the order (triggers outbox entry)
        mockMvc.perform(post("/api/v1/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(request)))
                .andExpect(status().isCreated());

        // Then: consume from Kafka and verify the event
        // Wait up to 15s for the outbox relay scheduler to fire
        try (KafkaConsumer<String, String> consumer = createTestConsumer()) {
            consumer.subscribe(List.of(KafkaTopics.ORDER_CREATED));

            ConsumerRecords<String, String> records = ConsumerRecords.empty();
            long deadline = System.currentTimeMillis() + 15_000;

            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records = consumer.poll(Duration.ofSeconds(1));
            }

            assertThat(records.isEmpty()).isFalse();

            var record = records.iterator().next();
            OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);

            assertThat(event.getCustomerId()).isEqualTo(customerId);
            assertThat(event.getTotalAmount()).isEqualByComparingTo("150.00");
            assertThat(event.getCurrency()).isEqualTo("USD");
            assertThat(event.getEventId()).isNotNull();
        }
    }

    // ── Test helpers ───────────────────────────────────────────────────────────

    private KafkaConsumer<String, String> createTestConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()
        ));
    }
}
