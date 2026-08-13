package com.mohammadanas.saga.notification.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The order reached its successful terminal state.
 *
 * <p>Fields follow the envelope in ARCHITECTURE.md section 5.5, plus the order details a
 * customer-facing message needs: who to tell, and what about.
 */
public record OrderConfirmedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String userId,
        String item,
        BigDecimal amount,
        Instant occurredAt) {
}
