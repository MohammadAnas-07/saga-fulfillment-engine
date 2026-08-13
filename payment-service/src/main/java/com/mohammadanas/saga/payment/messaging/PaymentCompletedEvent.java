package com.mohammadanas.saga.payment.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentCompletedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        UUID paymentId,
        BigDecimal amount,
        Instant occurredAt) {

    public static PaymentCompletedEvent from(UUID sagaId, UUID orderId, UUID paymentId, BigDecimal amount) {
        return new PaymentCompletedEvent(UUID.randomUUID(), sagaId, orderId, paymentId, amount, Instant.now());
    }
}
