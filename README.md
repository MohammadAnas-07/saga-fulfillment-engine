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

## Triggering a payment failure on purpose

Payment is simulated and **deterministic**, so the compensation path can be demonstrated
reliably rather than waited for. A payment fails when its amount is **strictly greater
than** `payment.simulation.failure-threshold` (default `1000.00`); anything at or below
succeeds.

To force the failure path, place an order whose **total** exceeds the threshold — note it
is the total that is charged, not the unit price:

```bash
curl -X POST http://localhost:8081/orders -H 'Content-Type: application/json' -d '{"userId":"user-1","itemSku":"MECH-KB-01","item":"Mechanical keyboard","quantity":2,"unitPrice":600.00}'
```

That is `600.00 x 2 = 1200.00`, above the threshold, so payment-service publishes
`PaymentFailed` and the saga compensates. To make *every* payment fail instead:

```bash
export PAYMENT_FAILURE_THRESHOLD=0
```

A random gateway was deliberately avoided — it would make the compensation tests flaky,
and compensation is the part of a saga worth testing.

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

### The Testcontainers problem is fixed (Chunk 8)

From Chunk 1 to Chunk 7 the integration tests **never executed once**. Docker Desktop's
engine returned a stubbed `400` on `/info` to docker-java, so Testcontainers rejected the
environment even though the `docker` CLI worked normally, and every one of those classes
skipped while the build stayed green.

The cause was simply a **Testcontainers version too old to negotiate the installed
Docker's API version**. Bumping `1.21.0` (Spring Boot 3.5.0's default) to `1.21.4` fixed it
outright — no `DOCKER_HOST` juggling, no `DOCKER_API_VERSION` pinning, no hand-rolled
container management. The version is pinned explicitly in the parent pom so a future Spring
Boot upgrade cannot silently walk it back:

```xml
<testcontainers.version>1.21.4</testcontainers.version>
```

Running those tests for the first time surfaced **three real defects** that had been sitting
in `main` — see the Chunk 8 entry in [ARCHITECTURE.md](ARCHITECTURE.md). Two of them would
have broken the system in production, not just in tests: three services could not serialize
or deserialize their own wire messages, because `jackson-datatype-jsr310` was missing and
every message carries an `Instant`.

### What is now genuinely verified, and what still is not

**Verified end to end, all six services running against real Kafka/Postgres/Redis**
(`integration-tests`, see ARCHITECTURE.md §8.4):

- the happy path, from `POST /orders` to the customer notification
- payment failure → compensation → stock restored → order `CANCELLED` → customer notified
- timeout → scheduler sweep → compensation → order `CANCELLED` → customer notified

**Not verified, and worth knowing before trusting the suite:**

- **Process isolation.** The six services run as six Spring contexts in one JVM, not six
  processes. Message flows, persistence and the state machine are real; independent
  deployment and mid-saga process death are not exercised.
- **§8.3 cases 6 and 7** — a late reply arriving after compensation, and a compensation that
  itself fails — have no end-to-end coverage. §8.3 itself calls these the two most often
  missed.
- **One open defect, found by these tests and not yet fixed:** ARCHITECTURE.md §8.5. A
  compensating `ReleaseInventory` can overtake the stale `ReserveInventory` it compensates,
  leaking stock on a cancelled order while every service believes it behaved correctly.

### Nothing skips any more

`mvn test` now runs **every** test in the repository, with `Skipped: 0` in every module.
There is no manual setup step and no test that quietly opts itself out.

scheduler-service's distributed-lock tests used to be the exception: written in Chunk 7
against a hand-started `docker run redis`, because Testcontainers could not reach Docker
then and the alternative was not testing the lock at all. Chunk 8 fixed Testcontainers, so
those tests manage their own Redis container like everything else.

If you see a non-zero `Skipped` count, something has regressed — that is now a signal
rather than the normal state of this build.

## Running

The `docker-compose` setup for Kafka/Postgres/Redis and proper run instructions land in
Chunk 9. Until then, the end-to-end suite is the way to see the whole system actually work
— it starts every dependency and all six services for you:

```bash
mvn -pl integration-tests -am test
```

**Note on jar names.** Each service builds two artifacts: `<module>.jar` is an ordinary
library jar, and **`<module>-exec.jar` is the runnable one**. The `exec` classifier exists
because `integration-tests` depends on the services as libraries, and a Spring Boot fat jar
cannot serve both purposes — see ARCHITECTURE.md §8.4. Chunk 9's run commands need the
`-exec` name.
