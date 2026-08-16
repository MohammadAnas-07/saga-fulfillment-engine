# saga-fulfillment-engine

An order fulfillment system built on the **orchestration-based Saga pattern**, with a
scheduler that drives timeout-based compensation for sagas that stall.

Six Spring Boot services talking over Kafka, each owning its own Postgres database, plus a
Redis-backed distributed lock. The whole thing runs with `docker compose up`.

> This is a **portfolio / learning project**. It is not production software, and the
> [Scope](#scope-what-is-and-is-not-here) section says exactly what that means. It also has
> one **known design defect**, described in [Known limitations](#known-limitations) rather
> than left for you to find.

---

## The problem it solves, and why the two halves belong together

Placing an order touches three services: stock has to be reserved, the customer has to be
charged, and the order has to be confirmed. Each owns its own database, so there is no
transaction spanning them. If the payment fails after stock is reserved, something has to
give the stock back.

**The Saga pattern** handles that: a sequence of local transactions, each with a
compensating action that undoes it. A single orchestrator decides what happens next, so the
workflow is readable in one place instead of being an emergent property of who subscribes to
what.

That gets you correctness when a step **fails**. It does nothing when a step **never
answers** — a consumer is down, a broker drops a message, a service dies mid-handler. The
saga simply sits there, and the stock it reserved stays reserved forever. Compensation is
only ever triggered by a reply that, in this case, is never coming.

**That is why the scheduler exists, and why it is not a second feature bolted on.** It is
the liveness half of the same guarantee: a watchdog sweeps for sagas past their deadline and
routes them into *the same* compensation path an explicit failure would take. The state
machine provides correctness; the scheduler provides the guarantee that a saga eventually
leaves a non-terminal state. Either alone is a system that can quietly leak resources.

The scheduler is built to run as more than one instance, which introduces its own problem:
two schedulers must not compensate the same saga at once. A **Redis lock keyed by saga id**
arbitrates, and that is the one thing in this project with a test that races 16 threads at a
real Redis to prove it.

---

## Architecture at a glance

```
                    ┌──────────────────┐
   POST /orders ───▶│  order-service   │──OrderCreated──┐
                    └──────────────────┘                │
                             ▲                          ▼
              ConfirmOrder / │                 ┌──────────────────┐
              CancelOrder    └─────────────────│ saga-orchestrator│◀── replies
                                               └──────────────────┘
                                                 │      │       ▲
                          ReserveInventory ──────┘      │       │
                          ReleaseInventory              │       │
                                 ▼                      ▼       │
                    ┌──────────────────┐    ┌──────────────────┐│
                    │inventory-service │    │ payment-service  │┘
                    └──────────────────┘    └──────────────────┘
                                                              
   order-service ──OrderConfirmed / OrderCancelled──▶ notification-service

   scheduler-service ──GET /internal/sagas/stuck──▶ saga-orchestrator
                     ──POST /internal/sagas/{id}/compensate──▶
```

- **Only `saga-orchestrator` decides the next step.** Every other service consumes a command
  addressed to it, performs one local transaction, and publishes a result event. No service
  reacts to another service's event on its own initiative.
- **Events are published by whoever owns the data.** The orchestrator issues the
  *command* to confirm an order; order-service announces the resulting *fact*.
- **Every consumer deduplicates on message id.** Kafka delivery is at-least-once, so
  redelivery is normal operation, not an error case.
- The scheduler's REST poll is the one deliberate exception to "no synchronous inter-service
  calls" — it is a liveness mechanism, and no saga ever waits on it.

**[ARCHITECTURE.md](ARCHITECTURE.md) is the real design document** — the state machine, the
topic contracts, the idempotency rules, the reasoning behind each decision, and the record
of which ones were revised and why.

---

## Running it

Requires Docker. Nothing else — the image build runs Maven for you.

```bash
docker compose up --build
```

That starts Postgres (five databases, one per service that needs one), Kafka in KRaft mode,
Redis, all six services, and two one-shot containers: one that creates all 15 Kafka topics
before any consumer starts, and one that seeds demo stock.

Startup ordering uses **real health checks**, not bare `depends_on` — Postgres is polled
with `pg_isready`, Kafka is asked to answer an actual API call, and the services that expose
HTTP are polled on `/actuator/health`. `depends_on` alone waits for a container to *start*,
which is not the same as it being able to answer you.

Wait for `order-service` to report healthy:

```bash
docker compose ps
```

**Give it a couple of minutes on a cold start.** The first `--build` compiles the whole
reactor inside the image (several minutes), and the services themselves take roughly 90–110
seconds each to start — six JVMs booting Spring, Hibernate and Kafka consumers on one
machine at once. The health checks have `start_period` set accordingly, so this is waiting,
not hanging.

### The happy path

```bash
curl -sS -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"userId":"user-1","itemSku":"MECH-KB-01","item":"Mechanical keyboard","quantity":2,"unitPrice":49.99}'
```

The response has the order id and `"status":"PENDING"`. The saga then runs on its own —
allow **10–15 seconds for the first order** while the consumer groups finish their initial
rebalance, and a second or two for every order after that:

```bash
curl -sS http://localhost:8081/orders/<ORDER_ID>          # -> "CONFIRMED"
docker compose logs notification-service | grep NOTIFICATION
```

```
NOTIFICATION [ORDER_CONFIRMED] to user user-1 for order <id>: Your order for Mechanical keyboard (99.98) is confirmed.
```

That one call crossed all six services: order created → saga started → stock reserved →
payment charged → saga confirmed → order confirmed → customer notified. You can watch the
stock move too:

```bash
docker compose exec postgres psql -U inventory_service -d inventory_db \
  -c "SELECT item_id, available_quantity, reserved_quantity FROM inventory;"
```

### The payment-failure path

payment-service declines any amount strictly above `PAYMENT_FAILURE_THRESHOLD` (default
`1000.00`), deterministically. Order above it:

```bash
curl -sS -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"userId":"user-2","itemSku":"MECH-KB-01","item":"Mechanical keyboard","quantity":2,"unitPrice":600.00}'
```

Stock is reserved, the payment is declined, compensation releases the stock, and the order
ends `CANCELLED` with a cancellation notification. Watch it happen:

```bash
docker compose logs -f saga-orchestrator inventory-service notification-service
```

The interesting assertion is not the final status but the **stock returning to its
pre-saga level** — a test that only checked the status would pass even if compensation
never ran.

### The timeout path

Stop the service that is supposed to reply, and shorten the saga deadline so you are not
waiting five minutes:

```bash
SAGA_TIMEOUT=PT20S SCHEDULER_INTERVAL=PT5S docker compose up -d saga-orchestrator scheduler-service
docker compose stop inventory-service

curl -sS -X POST http://localhost:8081/orders \
  -H 'Content-Type: application/json' \
  -d '{"userId":"user-3","itemSku":"MECH-KB-01","item":"Mechanical keyboard","quantity":1,"unitPrice":49.99}'
```

The saga reaches `AWAITING_INVENTORY` and stalls, because nothing is consuming
`ReserveInventory`. After ~20 seconds the scheduler's sweep finds it, takes its Redis lock,
and asks the orchestrator to compensate:

```bash
docker compose logs -f scheduler-service saga-orchestrator
```

The order ends `CANCELLED` and the customer is notified. Bring inventory-service back
(`docker compose start inventory-service`) and it works through its backlog — **and this is
where you can watch the known defect below actually happen.**

---

## Scope: what is and is not here

**Implemented and working:**

- The full saga: happy path, inventory-failure path, payment-failure path with compensation
- Timeout-based compensation driven by a Quartz sweep, coordinated by a Redis lock
- Idempotent message handling in every consumer, backed by a message-id primary key
- An append-only saga step history, so any saga can be explained after the fact
- `docker compose` deployment of the whole system

**Deliberately not here** — these are design decisions, not unfinished work:

- **No real payment gateway.** payment-service simulates outcomes against a threshold so the
  compensation paths can be exercised deterministically. Nothing here is PCI-compliant and
  none of it should be pointed at real money.
- **No real notifications.** notification-service renders a customer message and writes it to
  the log. `NotificationSender` is where an email or SMS channel would attach.
- **No service discovery.** There is no Eureka, Consul, or Spring Cloud dependency anywhere
  in this project and none is planned. Services find Kafka and their own database through
  configuration, and they never call each other directly — apart from the scheduler's one
  documented REST poll — so there is nothing for a registry to resolve.
- **No authentication, authorization, or multi-tenancy.** The `/internal` prefix on the
  orchestrator's API is documentation, not a security boundary.
- **No production hardening** — no rate limiting, no autoscaling, no DR plan, no TLS.
- **Not event sourcing.** Saga state is a relational table, deliberately, because that is the
  simpler and more explainable design.

---

## Known limitations

### Compensation can overtake the command it compensates

**This is a real defect, found by the end-to-end suite, and it is not fixed.**

When a saga times out while waiting on inventory, the orchestrator issues
`ReleaseInventory`. But the saga is in that state precisely *because* `ReserveInventory` has
not been consumed yet. Those two commands travel on **different topics**, and the per-saga
ordering guarantee — same key, same partition — only holds *within* one topic. Nothing
orders them relative to each other.

So the compensating command can be consumed first. Observed 62ms apart:

```
ReleaseInventory ... no active reservation to reverse; confirming compensation anyway
Reserved 2 of SKU-c46c81b3 for order 1eaa83ee-...  (8 available, 2 reserved)
Saga f6e999d2-... CANCELLED: compensation complete
```

The release found nothing to reverse and confirmed anyway — which is correct on its own
terms, and without it the saga would strand in `COMPENSATING` instead. The stale reserve then
took the stock. **The order is `CANCELLED`, the saga is `CANCELLED`, and the units stay
reserved forever**, with every service believing it behaved correctly.

Tracked as Chunk 10, with the evidence and a candidate fix, in
[ARCHITECTURE.md §8.5](ARCHITECTURE.md). The end-to-end timeout test deliberately does not
assert stock restoration on this path: asserting restoration would assert behaviour the
system does not have, and asserting the leak would enshrine it as correct.

### Other gaps worth knowing

- **The end-to-end suite runs six Spring contexts in one JVM**, not six processes. Message
  flows, persistence and the state machine are real; process isolation and mid-saga process
  death are not exercised there. `docker compose` is where the real deployment shape is
  proven.
- **Two failure cases have no end-to-end coverage**: a late reply arriving *after*
  compensation, and a compensation that itself fails. ARCHITECTURE.md §8.3 names these as the
  two most often missed, and they are still covered only at the single-service level.
- **Single-broker Kafka, single-node everything.** Replication factor 1 throughout.

---

## Modules

```
saga-fulfillment-engine/
├── pom.xml                  # parent — module list, dependency management
├── docker-compose.yml       # the whole system
├── Dockerfile               # one multi-stage build, parameterised per service
├── docker/                  # db init, topic creation, demo stock seeding
├── ARCHITECTURE.md          # the design document
│
├── order-service/           # Order aggregate + REST API. Publishes OrderCreated,
│                            #   OrderConfirmed, OrderCancelled
├── inventory-service/       # stock and reservations; compensating release
├── payment-service/         # payments (simulated); compensating refund
├── notification-service/    # terminal-state customer notifications (logged)
├── saga-orchestrator/       # the state machine — the only decision-maker
├── scheduler-service/       # timeout sweep + Redis distributed lock
└── integration-tests/       # end-to-end suite; no production code
```

Message contracts are duplicated per service rather than shared in a common module. That is
a deliberate trade — a `common-contracts` module is the intended escape hatch if the
duplication becomes painful — and it is why each producer has a contract test pinning its
JSON field names against the consumer's record. With no schema registry, a renamed field
arrives as `null` rather than failing to compile.

---

## Building and testing

```bash
mvn test
```

**154 tests. 154 pass. 0 skipped.**

That last number is the one worth checking. For six chunks of this project's history, the
integration tests never executed at all — Testcontainers could not reach the Docker daemon,
so they skipped silently while the build reported success. `BUILD SUCCESS` meant nothing.

That is fixed (it was a Testcontainers version too old to negotiate the installed Docker's
API version), and running those tests for the first time immediately surfaced **three real
defects that had been sitting in `main`** — two of them production bugs: three services
could not serialize or deserialize their own Kafka messages, because a Jackson module was
missing and every message carries a timestamp.

**A non-zero `Skipped` count in this repository is now a regression signal, not normal.**

| Module | Tests |
| --- | --- |
| order-service | 38 |
| inventory-service | 20 |
| payment-service | 21 |
| notification-service | 11 |
| saga-orchestrator | 42 |
| scheduler-service | 19 |
| integration-tests | 3 |

The integration and end-to-end tests need a reachable Docker daemon; they start their own
Postgres, Kafka and Redis containers. Unit tests need nothing.

The three end-to-end tests are the ones that matter most: they boot all six services against
real infrastructure and drive real orders through the happy path, the payment-failure path,
and a timeout — the last by genuinely stopping inventory-service's listeners, and advancing a
controllable clock rather than sleeping.

---

## A note on jar names

Each service builds two artifacts: `<module>.jar` is an ordinary library jar, and
**`<module>-exec.jar` is the runnable one**. The `exec` classifier exists because
`integration-tests` depends on the services as libraries, and a Spring Boot fat jar cannot
serve both purposes. The Dockerfile copies the `-exec` jar.
