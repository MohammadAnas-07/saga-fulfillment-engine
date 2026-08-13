package com.mohammadanas.saga.notification.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The order reached its unsuccessful terminal state, whether from a reservation failure,
 * a payment failure, or a timeout.
 *
 * <p>Deliberately carries no failure reason: the customer-facing message does not vary by
 * cause, and notification-service has no business branching on saga internals. Add one
 * only if a message genuinely needs to differ.
 */
public record OrderCancelledEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String userId,
        String item,
        BigDecimal amount,
        Instant occurredAt) {
}
