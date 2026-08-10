# saga-fulfillment-engine

An order fulfillment system in Java / Spring Boot demonstrating **distributed transactions
via the orchestration-based Saga pattern**, with a scheduler driving timeout-based
compensation.

> **Status: Chunk 0 — architecture and scaffolding.**
> The modules are empty skeletons. No business logic is implemented yet.

## What this demonstrates

- Saga pattern (**orchestration**, not choreography) for cross-service consistency
- Async event-driven communication over Kafka — no synchronous service-to-service calls
- Timeout-based compensation driven by a scheduler
- Idempotent message handling against at-least-once delivery
- Distributed locking via Redis so multiple scheduler instances stay safe

Payment processing is **simulated**. There is no real payment gateway integration — see
the non-goals in [ARCHITECTURE.md](ARCHITECTURE.md).

## Modules

| Module | Responsibility |
| --- | --- |
| `order-service` | Order aggregate, client REST API, publishes `OrderCreated` |
| `inventory-service` | Stock reservation and release (compensation) |
| `payment-service` | Simulated payment processing |
| `notification-service` | Customer notification on terminal states |
| `saga-orchestrator` | Saga state machine — the only component deciding the next step |
| `scheduler-service` | Timeout sweep + Redis lock, triggers compensation |

## Architecture

[ARCHITECTURE.md](ARCHITECTURE.md) is the design reference: saga flow step by step, both
failure paths, Kafka topic design, idempotency requirements, testing strategy, and the
chunk checklist tracking progress.

## Build

Requires JDK 21 and Maven 3.9+.

```bash
mvn clean install
```

## Running the tests

```bash
mvn test
```

Unit tests need no infrastructure. Integration tests use Testcontainers and need a
reachable Docker daemon.

> **Integration tests skip silently when Docker is unreachable.** `BUILD SUCCESS` alone
> does not mean they ran — check the surefire summary for a non-zero `Skipped` count.

Known issue on the current development machine: Docker Desktop's engine (API 1.55)
returns a stubbed `400` on `/info` to docker-java, so Testcontainers rejects the
environment even though the `docker` CLI works normally. Neither `DOCKER_HOST` pointed at
`npipe:////./pipe/dockerDesktopLinuxEngine` nor pinning `DOCKER_API_VERSION=1.44` helped.
Likely fixes, in order of preference: upgrade Testcontainers past the version that
supports API 1.55, or enable *Settings → General → Expose daemon on tcp://localhost:2375*
in Docker Desktop and set `DOCKER_HOST=tcp://localhost:2375`.

## Running

Not yet runnable — there is no business logic, and the Kafka/Postgres/Redis compose setup
lands in a later chunk. See the checklist in [ARCHITECTURE.md](ARCHITECTURE.md).
