package com.mohammadanas.saga.payment.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Compensation is confirmed. Together with {@code InventoryReleased}, this is what lets
 * the orchestrator move a saga from {@code COMPENSATING} to {@code CANCELLED}
 * (ARCHITECTURE.md section 3.3).
 *
 * <p>Published on <strong>every</strong> terminal outcome of the refund handler,
 * including the case where there was no successful payment to reverse. Staying silent
 * there would leave the orchestrator waiting forever on a saga that never took the
 * customer's money — the common case, since most compensations follow a payment that
 * failed.
 *
 * <p>The one deliberate exception is a redelivered {@code messageId}: that is not a new
 * outcome, it is the same outcome arriving twice, which is precisely what idempotency
 * exists to suppress.
 *
 * @param paymentId the reversed payment, or {@code null} when {@code outcome} is
 *                  {@code NOTHING_TO_REVERSE}.
 * @param amount    the sum returned, or {@code null} when there was nothing to reverse —
 *                  deliberately null rather than zero, which would read as "refunded
 *                  nothing" instead of "no refund applied".
 */
public record PaymentRefundedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        CompensationOutcome outcome,
        Instant occurredAt) {

    /** Confirmation that real money was handed back. */
    public static PaymentRefundedEvent reversed(UUID sagaId, UUID orderId, UUID paymentId, BigDecimal amount) {
        return new PaymentRefundedEvent(
                UUID.randomUUID(), sagaId, orderId, paymentId, amount,
                CompensationOutcome.REVERSED, Instant.now());
    }

    /** Confirmation that compensation is complete because there was nothing to undo. */
    public static PaymentRefundedEvent nothingToReverse(UUID sagaId, UUID orderId) {
        return new PaymentRefundedEvent(
                UUID.randomUUID(), sagaId, orderId, null, null,
                CompensationOutcome.NOTHING_TO_REVERSE, Instant.now());
    }
}
