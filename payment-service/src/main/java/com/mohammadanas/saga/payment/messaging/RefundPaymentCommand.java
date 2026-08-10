package com.mohammadanas.saga.payment.messaging;

import java.util.UUID;

/**
 * The compensating command: reverse this order's successful payment.
 *
 * <p>Carries no amount — the payment row records what was taken, so a replayed or
 * hand-crafted refund cannot return a different figure than was charged.
 *
 * <p>Needed because a saga can fail <em>after</em> payment succeeded: the reply may be
 * lost and the scheduler's timeout sweep (ARCHITECTURE.md section 4) then compensates a
 * saga that did, in fact, take the customer's money.
 */
public record RefundPaymentCommand(
        UUID messageId,
        UUID sagaId,
        UUID orderId) {
}
