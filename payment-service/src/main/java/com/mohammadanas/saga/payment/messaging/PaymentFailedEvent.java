package com.mohammadanas.saga.payment.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Payment did not go through. Drives the saga into {@code COMPENSATING}: inventory was
 * already reserved by this point and must be released (ARCHITECTURE.md section 3.3).
 *
 * <p>No money moved, so this path needs no refund — the reserved stock is the only thing
 * to undo.
 */
public record PaymentFailedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        PaymentFailureReason reason,
        Instant occurredAt) {

    public static PaymentFailedEvent from(
            UUID sagaId, UUID orderId, UUID paymentId, BigDecimal amount, PaymentFailureReason reason) {
        return new PaymentFailedEvent(
                UUID.randomUUID(), sagaId, orderId, paymentId, amount, reason, Instant.now());
    }
}
