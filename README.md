# Event-Driven Order System

A production-grade microservices portfolio project demonstrating event-driven architecture with Apache Kafka (KRaft mode), the Transactional Outbox Pattern, idempotent processing, Dead Letter Queues and horizontal scaling with Docker.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Client (HTTP)                            │
└─────────────────────────────┬───────────────────────────────────┘
                              │ POST /api/v1/orders
                              ▼
                    ┌─────────────────┐
                    │      Nginx      │  :8084
                    │  Load Balancer  │
                    └────────┬────────┘
                             │ round-robin
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
  order-service-1    order-service-2    order-service-3
      :8081               :8081               :8081
          │                  │                  │
          └──────────────────┴──────────────────┘
                             │ Transactional Outbox
                             ▼
                    ┌─────────────────┐
                    │   order-db      │  PostgreSQL :5432
                    │ orders +        │
                    │ outbox_events   │
                    └────────┬────────┘
                             │ Outbox Relay (@Scheduled every 5s)
                             ▼
                    ┌──────────────────┐
                    │  Kafka (KRaft)   │  :9092
                    │ order.created    │  3 partitions
                    │ payment.completed│  3 partitions
                    └────────┬─────────┘
               ┌─────────────┴─────────────┐
               ▼                           ▼
      ┌─────────────────┐       ┌───────────────────────┐
      │ payment-service │       │ notification-service  │
      │     :8082       │       │       :8083           │
      └────────┬────────┘       └───────────────────────┘
               │
               ▼
      ┌─────────────────┐       ┌───────────────────────┐
      │   payment-db    │       │    order-service      │
      │ PostgreSQL:5433 │       │ also consumes         │
      └─────────────────┘       │ payment.completed     │
                                │ → updates status      │
                                └───────────────────────┘
```

---

## Tech Stack

| Technology | Version               | Purpose |
|---|-----------------------|---|
| Java | 22                    | Language |
| Spring Boot | 3.3.0                 | Application framework |
| Apache Kafka | 3.6 (Confluent 7.6.0) | Async messaging — KRaft mode |
| PostgreSQL | 16                    | Persistence |
| Docker + Compose | -                     | Containerization |
| Nginx | Alpine                | Load balancing |
| Maven | 3.9                   | Build tool |

---

## Modules

```
event-driven-order-system/
├── shared-events/          ← Shared Kafka event 
├── order-service/          ← REST API + Outbox Pattern + Status updates
├── payment-service/        ← Kafka consumer + idempotent payment processing
├── notification-service/   ← Kafka consumer + simulated email notifications
└── docker/
    ├── docker-compose.yml
    └── nginx.conf
```

---

## Architecture Patterns

### 1. Transactional Outbox Pattern
Order and outbox event are written in a **single ACID transaction**. A scheduler relays events to Kafka every 5 seconds. This solves the dual-write problem. If the app crashes after saving the order but before publishing to Kafka, the event remains in the DB and will be retried automatically.

```
createOrder() {
    BEGIN TRANSACTION
        INSERT INTO orders (...)         ← order saved
        INSERT INTO outbox_events (...)  ← event queued atomically
    COMMIT
}

@Scheduled(fixedDelay = 5000)
relayOutboxEvents() {
    SELECT ... FOR UPDATE SKIP LOCKED   ← safe for multiple instances
    kafkaTemplate.send(...)
    UPDATE outbox_events SET processed = true
}
```

### 2. FOR UPDATE SKIP LOCKED
When 3 Order Service instances all run the outbox relay simultaneously, `FOR UPDATE SKIP LOCKED` ensures each instance processes a **different** batch of events - no duplicate Kafka messages, no race conditions.

### 3. Idempotent Processing
Payment Service uses a `UNIQUE` constraint on `source_event_id` to prevent double-charging if the same event is delivered more than once (Kafka at-least-once delivery guarantee). If a duplicate arrives, the existing payment is returned without reprocessing.

```java
var existing = paymentRepository.findBySourceEventId(event.getEventId());
if (existing.isPresent()) {
    return buildPaymentCompletedEvent(existing.get()); // no double charge
}
```

### 4. Dead Letter Queue (DLQ)
Failed messages after retry exhaustion are routed to DLQ topics for investigation and manual replay:
- `order.created.dlq` — Payment Service failures (3 retries, 2s FixedBackOff)
- `payment.completed.dlq` — Notification Service failures (ExponentialBackOff)

### 5. Manual Offset Acknowledgement
Kafka offsets are only committed **after successful processing** (`MANUAL_IMMEDIATE` ack mode). If the service crashes mid-processing, the message is re-delivered - nothing is lost.

### 6. Database-per-Service
Order Service and Payment Service each have their own isolated PostgreSQL database. They never share data directly - all communication goes through Kafka topics.

### 7. Order Status Feedback Loop
Order Service also consumes `payment.completed` in its own consumer group (`order-service-payment-group`), closing the feedback loop by updating order status based on payment result.

### 8. KRaft (Kafka without Zookeeper)
Kafka runs in KRaft mode - the modern architecture where Kafka manages its own cluster metadata using the Raft consensus algorithm, without requiring a separate Zookeeper instance. KRaft is production-ready since Kafka 3.3 and Zookeeper support was fully removed in Kafka 4.0.

```yaml
# KRaft replaces all of this:
# ❌ zookeeper service
# ❌ KAFKA_ZOOKEEPER_CONNECT
# ❌ zookeeper-data volume

# ✅ KRaft config instead:
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:29093
CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
```

---

## Order Lifecycle

```
POST /api/v1/orders
       │
       ▼
   PENDING  ──→ outbox_events table
                       │
                       │ @Scheduled relay (every 5s)
                       ▼
              Kafka: order.created
                       │
                       ▼
               Payment Service
               processes payment
                       │
              ┌────────┴────────┐
              ▼                 ▼
          SUCCESS            FAILED
              │                 │
              └────────┬────────┘
                       ▼
              Kafka: payment.completed
                       │
              ┌────────┴──────────────────────┐
              ▼                               ▼
    Notification Service            Order Service updates
    sends simulated email           order status:
                                    SUCCESS → CONFIRMED
                                    FAILED  → CANCELLED
```

---

## Kafka Topics

| Topic | Producer | Consumer(s) | Partitions |
|---|---|---|---|
| `order.created` | Order Service (Outbox) | Payment Service | 3 |
| `payment.completed` | Payment Service | Notification Service, Order Service | 3 |
| `order.created.dlq` | Payment Service (on error) | — | 1 |
| `payment.completed.dlq` | Notification Service (on error) | — | 1 |

---

## Consumer Groups

| Group ID | Consumes | Purpose |
|---|---|---|
| `payment-service-group` | `order.created` | Process payments |
| `notification-service-group` | `payment.completed` | Send notifications |
| `order-service-payment-group` | `payment.completed` | Update order status |

Each group receives an independent copy of all messages - none interferes with the others.

---

## API Endpoints

Base URL: `http://localhost:8084/api/v1`

### Create Order
```http
POST /orders
Content-Type: application/json

{
  "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "totalAmount": 150.00,
  "currency": "USD"
}
```

Response `201 Created`:
```json
{
  "id": "034f722f-f4c0-4d3d-9eaa-dd4a8c82382e",
  "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "totalAmount": 150.00,
  "currency": "USD",
  "status": "PENDING",
  "createdAt": "2026-03-20T10:36:31Z"
}
```

### Get Order by ID
```http
GET /orders/{orderId}
```

### Health Check
```http
GET /actuator/health
```

---

## Payment Simulation

The Payment Service uses a simulated gateway with a predictable rule:

| Amount | Result | Order Status |
|---|---|---|
| Ends in `.99` (e.g. `149.99`) | ❌ FAILED | `CANCELLED` |
| Any other amount (e.g. `150.00`) | ✅ SUCCESS | `CONFIRMED` |

---

## Verified Tests

### ✅ Test 1 — Race Condition / No Duplicates
10 parallel requests sent simultaneously across 3 Order Service instances.

```
orders:   CONFIRMED | 11   ← exact match, no missing orders
payments: SUCCESS   | 11   ← exact match, no duplicate payments
```

Proves that `FOR UPDATE SKIP LOCKED` correctly prevents multiple instances from processing the same outbox event.

### ✅ Test 2 — Idempotency
Same `order.created` event published twice to Kafka with the same `eventId`.

```sql
SELECT source_event_id, COUNT(*) FROM payments
GROUP BY source_event_id;
-- Every eventId appears exactly once despite being delivered twice
```

Proves the `UNIQUE` constraint on `source_event_id` prevents double-charging.

### ✅ Test 3 — Dead Letter Queue
Invalid JSON published directly to `order.created` topic.

```bash
echo "invalid-json" | docker exec -i kafka kafka-console-producer \
  --broker-list kafka:29092 --topic order.created

# After 3 retries (3 × 2s), check DLQ:
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic order.created.dlq --from-beginning
# → "invalid-json" appears in DLQ ✅
```

Proves that unprocessable messages are safely routed to DLQ after retry exhaustion.

---

## Running Locally

### Prerequisites
- Docker Desktop
- Java 22
- Maven 3.9+

### Start Everything

```bash
cd docker

# Clean start (wipes all data)
docker compose down -v

# Build all service images
docker compose build

# Start with 3 Order Service instances
docker compose up -d --scale order-service=3
```

Wait ~90 seconds for Spring Boot to start inside Docker.

### Verify Services Are Up

```bash
docker compose ps
```

Check health endpoint:
```bash
curl http://localhost:8084/actuator/health
# → {"status":"UP"}
```

### Test the Full Flow

**Windows (PowerShell)** - use a file to avoid quote escaping issues:
```powershell
'{"customerId":"3fa85f64-5717-4562-b3fc-2c963f66afa6","totalAmount":150.00,"currency":"USD"}' | Out-File -FilePath "C:\temp\order.json" -Encoding utf8 -NoNewline
curl.exe -X POST http://localhost:8084/api/v1/orders -H "Content-Type: application/json" -d "@C:\temp\order.json"
```

Wait 5-10 seconds, then check status:

```bash
docker exec -it order-db psql -U order_user -d order_db \
  -c "SELECT id, status, created_at FROM orders ORDER BY created_at DESC LIMIT 5;"
```

### Watch Live Logs

```bash
docker compose logs -f order-service payment-service notification-service
```

### Scale Up / Down

```bash
docker compose up -d --scale order-service=5
docker compose up -d --scale order-service=1
```

### Stop Cleanly

```bash
# Stop without wiping data
docker compose down

# Stop and wipe all data (full reset)
docker compose down -v
```

## Access Points

| Service | URL |
|---|---|
| Order Service API (via Nginx) | http://localhost:8084 |
| Payment Service | http://localhost:8082 |
| Notification Service | http://localhost:8083 |
| Kafka UI | http://localhost:8090 |
| Order DB | localhost:5432 |
| Payment DB | localhost:5433 |

---

## Monitoring with Kafka UI

Open **http://localhost:8090** to inspect:

- **Dashboard** — cluster health, broker count, partition status
- **Topics** — message count, partition distribution, DLQ message count
- **Messages** — full JSON payload of each Kafka event
- **Consumers** — consumer group lag (should be 0 after processing)

---

## Project Structure

```
order-service/
└── src/main/java/com/orderSystem/orderservice/
    ├── controller/
    │   └── OrderController.java              ← REST API endpoints
    ├── service/
    │   ├── OrderService.java                 ← Business logic + Outbox write + Status update
    │   └── OutboxRelayService.java           ← Scheduled Kafka publisher
    ├── kafka/
    │   ├── config/
    │   │   ├── KafkaProducerConfig.java      ← Producer (acks=all, idempotent)
    │   │   ├── KafkaConsumerConfig.java      ← Consumer (MANUAL_IMMEDIATE)
    │   │   └── KafkaTopicConfig.java         ← Topic declarations
    │   └── consumer/
    │       └── PaymentCompletedEventConsumer.java  ← Updates order status
    ├── entity/
    │   ├── Order.java
    │   ├── OrderStatus.java                  ← PENDING / CONFIRMED / CANCELLED
    │   └── OutboxEvent.java
    ├── repository/
    │   ├── OrderRepository.java
    │   └── OutboxEventRepository.java        ← FOR UPDATE SKIP LOCKED query
    ├── dto/
    │   ├── CreateOrderRequest.java
    │   └── OrderResponse.java
    └── exception/
    │   ├── GlobalExceptionHandler.java
    │   └── OrderNotFoundException.java
    └── OrderServiceApplication.java    

payment-service/
└── src/main/java/com/orderSystem/paymentservice/
    ├── kafka/
    │   ├── config/
    │   │   └── KafkaConsumerConfig.java      ← 3 retries × 2s FixedBackOff + DLQ
    │   └── consumer/
    │       └── OrderCreatedEventConsumer.java ← Processes payments, publishes result
    ├── service/
    │   └── PaymentService.java               ← Idempotency check + payment logic
    ├── entity/
    │   ├── Payment.java
    │   └── PaymentStatus.java                ← SUCCESS / FAILED / PENDING
    └── repository/
    │   └── PaymentRepository.java            ← findBySourceEventId (idempotency)
    └── PaymentServiceApplication.java  

notification-service/
└── src/main/java/com/orderSystem/notificationservice/
    ├── kafka/
    │   ├── config/
    │   │   └── KafkaConsumerConfig.java      ← ExponentialBackOff + DLQ
    │   └── consumer/
    │       └── PaymentCompletedEventConsumer.java
    └── service/
    │    └── NotificationService.java          ← Simulated email (SUCCESS / FAILED)
    └── NotificationServiceApplication.java
 
shared-events/
└── src/main/java/com/orderSystem/shared/
    ├── events/
    │   ├── OrderCreatedEvent.java
    │   └── PaymentCompletedEvent.java
    └── config/
        └── KafkaTopics.java              ← Topic name constants
```

---

## Key Design Decisions

**Why Transactional Outbox instead of direct Kafka publish?**
Direct Kafka publish inside a DB transaction is not atomic - if Kafka is down or the app crashes after the DB commit but before the Kafka send, the event is lost forever. The Outbox Pattern guarantees the event is always persisted first and will eventually reach Kafka regardless of failures.

**Why KRaft instead of Zookeeper?**
Zookeeper is deprecated and was fully removed in Kafka 4.0. KRaft is simpler (one less service to run), faster to start, and is the industry standard going forward. It uses the Raft consensus algorithm built directly into Kafka brokers to manage cluster metadata.

**Why `FOR UPDATE SKIP LOCKED`?**
With 3 Order Service instances all running the outbox relay every 5 seconds, a naive `SELECT WHERE processed = false` would return the same rows to all 3 instances simultaneously, causing duplicate Kafka messages. `SKIP LOCKED` makes each instance atomically claim a different batch of rows.

**Why `MANUAL_IMMEDIATE` acknowledgment?**
Auto-commit marks messages as consumed before processing completes. If the service crashes mid-processing, those messages are permanently lost. Manual ack ensures the Kafka offset is only committed after the business logic fully succeeds.

**Why a separate consumer group for Order Service on `payment.completed`?**
Each consumer group receives a completely independent copy of all messages. `payment-service-group`, `notification-service-group`, and `order-service-payment-group` all independently consume every `payment.completed` event - they don't compete with each other, and removing one group doesn't affect the others.

**Why FixedBackOff for Payment Service and ExponentialBackOff for Notification Service?**
Payment failures are typically deterministic (bad data) and should be retried quickly and uniformly. Notification failures are often caused by external service overload - exponential backoff reduces pressure on the email provider progressively.