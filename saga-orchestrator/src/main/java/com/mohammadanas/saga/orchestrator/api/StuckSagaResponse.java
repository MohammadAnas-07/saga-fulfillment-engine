package com.mohammadanas.saga.orchestrator.api;

import com.mohammadanas.saga.orchestrator.domain.Saga;
import com.mohammadanas.saga.orchestrator.domain.SagaStatus;
import java.time.Instant;
import java.util.UUID;

/**
 * What the scheduler needs to decide whether to compensate a saga, and nothing more.
 *
 * <p>Deliberately not the {@code Saga} entity: exposing that would let the scheduler grow
 * a dependency on the orchestrator's internal schema, which is exactly what querying the
 * database directly would have done.
 */
public record StuckSagaResponse(
        UUID sagaId,
        UUID orderId,
        SagaStatus status,
        Instant timeoutDeadline) {

    public static StuckSagaResponse from(Saga saga) {
        return new StuckSagaResponse(
                saga.getId(), saga.getOrderId(), saga.getStatus(), saga.getTimeoutDeadline());
    }
}
