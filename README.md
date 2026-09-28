# NovaBankKotlin

A Kotlin / Spring Boot demo of reliable bank-transfer event processing:
a REST API accepts transfer requests, stores them with the **transactional outbox** pattern,
publishes them to **Kafka**, and processes them in an **idempotent consumer** with retries and a **dead-letter topic (DLT)**.

It is a Kotlin rewrite of the Java project [NovaBank](https://github.com/dariuszchmiela/NovaBank),
targeting Spring Boot 4, Spring Kafka 4 and Jackson 3.

## Architecture

```
POST /api/transfers
        │
        ▼
TransferController ── validation (ProblemDetail 400)
        │
        ▼
TransferProducerService ── one DB transaction
        │
        ▼
PostgreSQL: outbox_events (published_at = NULL)
        │
        ▼
OutboxPublisherScheduler ── polls, sends, waits for Kafka ack, sets published_at
        │
        ▼
Kafka: bank.transfers.requested (key = sourceAccountId)
        │
        ▼
TransferConsumerService ── @KafkaListener, manual ack
        │
        ▼
PostgreSQL: processed_transfers (PK = transfer_id)

   processing keeps failing (after 3 retries) ──┐
   payload cannot be deserialized ──────────────┴──▶ Kafka: bank.transfers.requested.dlt
```

- **Transactional outbox**: the HTTP request never talks to Kafka. The event is written to `outbox_events` in the same
  database transaction as the request handling, so an accepted transfer cannot be lost if Kafka is unavailable.
- **At-least-once delivery**: the scheduler marks an outbox row as published only after the Kafka ack, so a crash in
  between re-publishes the event.
- **Idempotency**: the consumer treats `processed_transfers.transfer_id` (primary key) as the deduplication guard,
  so a redelivered event does not create a second transfer.
- **DLT**: messages that keep failing, or cannot be deserialized at all, are moved to a dead-letter topic instead of
  blocking the partition.

## Tech stack

| Area | Technology |
|---|---|
| Language | Kotlin 2.3.21 on Java 25 |
| Framework | Spring Boot 4.1.1 (Spring Framework 7.0.9) |
| Messaging | Spring Kafka 4.1.1, Kafka clients 4.2.1 |
| JSON | Jackson 3.1.5 (`tools.jackson`) with `jackson-module-kotlin` |
| Persistence | PostgreSQL, Spring Data JPA / Hibernate 7.4.5, Liquibase 5.0.3 (Hibernate only validates the schema) |
| Testing | JUnit 6, Mockito, MockMvc, Testcontainers 2.0.5 (PostgreSQL + Kafka), Awaitility 4.3.0 |
| Build / CI | Maven, GitHub Actions |

Library versions come from the Spring Boot 4.1.1 dependency management. Local and test infrastructure uses the
`postgres:18-alpine` and `confluentinc/cp-kafka:7.6.1` (KRaft) images.

## API

### `POST /api/transfers`

API versioning is **header-based** (Spring MVC API versioning): `X-API-Version: 1`.
The default version is `1`, so the header is optional.

```http
POST /api/transfers
X-API-Version: 1
Content-Type: application/json

{
  "sourceAccountId": "ACC-001",
  "targetAccountId": "ACC-002",
  "amount": 150.00,
  "currency": "PLN"
}
```

Response `202 Accepted`. The transfer is queued for asynchronous processing:

```json
{
  "transferId": "3f1c1a52-5d0e-4c1e-9d55-2b0f6a3f9e41",
  "status": "ACCEPTED"
}
```

All fields are required (`@NotBlank` / `@NotNull`). An invalid request returns `400 Bad Request` as `ProblemDetail`
(`application/problem+json`), with the first message for each invalid field under `errors`:

```json
{
  "detail": "One or more fields are invalid",
  "instance": "/api/transfers",
  "status": 400,
  "title": "Validation failed",
  "errors": {
    "sourceAccountId": "must not be blank"
  }
}
```

## Reliability model

1. The HTTP request does not publish to Kafka directly.
2. `TransferProducerService` serializes a `TransferRequestedEvent` and inserts it into `outbox_events` within a
   single DB transaction, then returns the `transferId`.
3. `OutboxPublisherScheduler` (every `novabank.outbox.poll-interval-ms`, default 1 s) reads unpublished rows oldest
   first and sends the stored JSON payload as-is (key = source account ID, so one account's transfers stay ordered).
4. It waits for the Kafka ack (up to `novabank.outbox.publish-timeout-seconds`, default 5 s) and only then sets
   `published_at`. A failed or timed-out send leaves the row unpublished and it is retried on the next poll.
5. A crash between the Kafka ack and the DB commit therefore publishes the same event again. **Transport is
   at-least-once.**
6. The consumer is idempotent. Every event is **inserted** into `processed_transfers` (the entity is always treated
   as new, so a duplicate is never merged over the existing row).
7. A redelivered `transferId` violates the `processed_transfers.transfer_id` primary key. The consumer confirms the row
   exists, logs the duplicate and acknowledges it. The database constraint also covers concurrent redeliveries.
8. Failing messages are retried and then isolated in the DLT, so a poison message cannot block the partition.

Together this gives an **effectively-once business outcome**: each transfer is recorded exactly once, even though
messages can be delivered more than once. This is not Kafka exactly-once semantics (no Kafka transactions are used).

## Kafka error handling

- **Deserialization**: key and value use `ErrorHandlingDeserializer`, delegating to `StringDeserializer` and
  Spring Kafka 4's Jackson 3 `JacksonJsonDeserializer` (target type `TransferRequestedEvent`, type headers ignored).
  A malformed payload does not crash the consumer; the failure is passed on to the error handler.
- **Acknowledgment**: `ack-mode: manual_immediate` and `enable-auto-commit: false`. The listener acknowledges only
  after the transfer is stored, or after a confirmed duplicate.
- **Retry**: `DefaultErrorHandler` with `FixedBackOff`, **3 retries, 1 s apart** (4 attempts in total).
- **DLT**: after retries are exhausted, `DeadLetterPublishingRecoverer` publishes the record to
  `bank.transfers.requested.dlt` on the same partition, with the standard `kafka_dlt-*` exception headers.
  Deserialization failures are not retryable and go to the DLT immediately.
- **Raw payload preservation**: for a deserialization failure the recoverer forwards the original `byte[]`. The
  producer used by the recoverer serializes `byte[]` with `ByteArraySerializer` and everything else as JSON, so the
  DLT receives the exact original bytes rather than a re-encoded value.
- The outbox publisher uses a separate `String`/`String` template, because the payload in the database is already JSON.

## Tests

```bash
mvn test
```

Integration tests need a running **Docker daemon**. PostgreSQL and Kafka are started once per test run with
Testcontainers, and all integration tests share one Spring context.

| Test | Scope |
|---|---|
| `TransferProducerServiceTest` | unit: outbox row content (topic, key, payload, `publishedAt == null`) |
| `OutboxPublisherSchedulerTest` | unit: publish success, send failure, timeout, continue after a failure, `@Scheduled`/`@Transactional` configuration |
| `TransferConsumerServiceTest` | unit: save + ack, duplicate → ack, real DB error → no ack, invalid UUID → no ack |
| `TransferControllerIntegrationTest` | HTTP: 202 + outbox row, 400 `ProblemDetail` on validation failure |
| `OutboxPublisherSchedulerIntegrationTest` | outbox → Kafka: exact key and payload, `published_at` set, no re-publish |
| `TransferConsumerServiceIntegrationTest` | Kafka → DB: first delivery, duplicate is skipped without overwriting, next message still processed |
| `TransferConsumerDltIntegrationTest` | processing failure → retries → DLT; malformed JSON → DLT with original bytes |
| `TransferFlowEndToEndTest` | full flow: HTTP → `outbox_events` → scheduler → Kafka → listener → `processed_transfers` |
| `NovaBankKotlinApplicationTests` | context loads with Liquibase + Hibernate schema validation |

Scheduled polling is disabled in tests by a very long interval; the tests call `publishPendingEvents()` explicitly
to stay deterministic. Asynchronous results are awaited with Awaitility or a Kafka poll loop with a deadline, never
with `sleep`.

## Running locally

```bash
docker compose up -d      # PostgreSQL on 5432, Kafka on 9092
mvn spring-boot:run       # or run NovaBankKotlinApplication from the IDE
```

Liquibase creates the schema on startup. Then:

```bash
curl -i -X POST http://localhost:8080/api/transfers \
  -H "Content-Type: application/json" \
  -H "X-API-Version: 1" \
  -d '{"sourceAccountId":"ACC-001","targetAccountId":"ACC-002","amount":150.00,"currency":"PLN"}'
```

Within a few seconds the event is published by the scheduler and a row appears in `processed_transfers`
(the first message after startup also waits for Kafka to auto-create the topic).

## CI

GitHub Actions (`.github/workflows/ci.yml`) runs the full `mvn --batch-mode test` on JDK 25 (Temurin) for every push
and pull request to `master`. Testcontainers uses the Docker daemon of the Ubuntu runner, so no extra services are
configured in the workflow.
