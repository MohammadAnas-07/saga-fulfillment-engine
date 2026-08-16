package com.mohammadanas.saga.scheduler.orchestrator;

import java.time.Instant;
import java.util.UUID;

/**
 * One stuck saga, as the orchestrator reports it.
 *
 * <p>Mirrors saga-orchestrator's {@code StuckSagaResponse} field for field. The two modules
 * share no code (§7), so this is the same duplicated-contract situation as every other
 * cross-service payload here, and {@code StuckSagaContractTest} pins the field names for
 * the same reason.
 *
 * <p>{@code status} is deliberately a {@code String} rather than a copy of the
 * orchestrator's {@code SagaStatus} enum. The scheduler never branches on it — deciding
 * what a given status needs is the orchestrator's job (§4, §5.1) — so it is carried for
 * logging only, and a String cannot fail to deserialize when the orchestrator adds a state
 * this service has never heard of.
 */
public record StuckSaga(
        UUID sagaId,
        UUID orderId,
        String status,
        Instant timeoutDeadline) {
}
