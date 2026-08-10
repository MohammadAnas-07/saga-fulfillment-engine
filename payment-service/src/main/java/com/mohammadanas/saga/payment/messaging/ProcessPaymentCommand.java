package com.mohammadanas.saga.payment.messaging;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Command from saga-orchestrator: charge this order.
 *
 * <p>{@code amount} is the order total as order-service derived it. payment-service does
 * not recompute it.
 *
 * <p>{@code messageId} is the idempotency key — see
 * {@link com.mohammadanas.saga.payment.domain.ProcessedCommand}.
 */
public record ProcessPaymentCommand(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String userId,
        BigDecimal amount) {
}
