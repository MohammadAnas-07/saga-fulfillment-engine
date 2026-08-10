package com.mohammadanas.saga.order.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published when an order enters {@code PENDING}. This is what starts a saga —
 * saga-orchestrator consumes it and creates the saga record.
 *
 * <p>Carries no {@code sagaId}: the saga does not exist yet at publish time, so these
 * messages are keyed by {@code orderId} instead (ARCHITECTURE.md section 5.4).
 */
public record OrderCreatedEvent(
        UUID messageId,
        UUID orderId,
        String userId,
        String item,
        BigDecimal amount,
        Instant occurredAt) {

    public static OrderCreatedEvent from(UUID orderId, String userId, String item, BigDecimal amount) {
        return new OrderCreatedEvent(UUID.randomUUID(), orderId, userId, item, amount, Instant.now());
    }
}
