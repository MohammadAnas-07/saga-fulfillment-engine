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
 *
 * <p>{@code itemSku} and {@code quantity} are what the orchestrator forwards into
 * {@code ReserveInventory}; {@code amount} is what it forwards into {@code ProcessPayment}.
 * {@code unitPrice} rides along so a consumer can audit the total rather than having to
 * take it on trust — {@code amount} is always {@code unitPrice * quantity}
 * (ARCHITECTURE.md section 2.1).
 */
public record OrderCreatedEvent(
        UUID messageId,
        UUID orderId,
        String userId,
        String itemSku,
        String item,
        int quantity,
        BigDecimal unitPrice,
        BigDecimal amount,
        Instant occurredAt) {

    public static OrderCreatedEvent from(
            UUID orderId,
            String userId,
            String itemSku,
            String item,
            int quantity,
            BigDecimal unitPrice,
            BigDecimal amount) {
        return new OrderCreatedEvent(
                UUID.randomUUID(), orderId, userId, itemSku, item, quantity, unitPrice, amount, Instant.now());
    }
}
