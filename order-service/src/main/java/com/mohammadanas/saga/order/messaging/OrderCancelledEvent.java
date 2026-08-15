package com.mohammadanas.saga.order.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published when an order reaches {@code CANCELLED}, from any of the paths that get there
 * — reservation failure, payment failure, or timeout (ARCHITECTURE.md sections 3.2, 3.3
 * and 4). Consumed by notification-service.
 *
 * <p>Carries no failure reason, deliberately: order-service does not know one. It applies
 * the {@code CancelOrder} command it was given and holds no saga state, and
 * notification-service's message does not vary by cause anyway — its own record has no
 * such field either.
 *
 * <p>As with {@link OrderConfirmedEvent}, the field names are a wire contract with
 * notification-service's record and are pinned by {@code TerminalEventContractTest}.
 */
public record OrderCancelledEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String userId,
        String item,
        BigDecimal amount,
        Instant occurredAt) {

    public static OrderCancelledEvent from(
            UUID sagaId, UUID orderId, String userId, String item, BigDecimal amount) {
        return new OrderCancelledEvent(
                UUID.randomUUID(), sagaId, orderId, userId, item, amount, Instant.now());
    }
}
