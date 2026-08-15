package com.mohammadanas.saga.order.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Published when an order reaches {@code CONFIRMED}. Consumed by notification-service,
 * which tells the customer.
 *
 * <p>order-service publishes this and not saga-orchestrator: an order reaching
 * {@code CONFIRMED} is a fact about the order aggregate, and events belong to the service
 * that owns the data (ARCHITECTURE.md sections 3.1 and 5.1). The orchestrator issues the
 * {@code ConfirmOrder} command; announcing the resulting fact is order-service's.
 *
 * <p><strong>The field names here are a wire contract</strong> with
 * notification-service's own {@code OrderConfirmedEvent} record, matched field for field.
 * The wire is JSON resolved against the consumer's declared type, so a renamed field does
 * not fail to compile — it arrives there as {@code null}. {@code TerminalEventContractTest}
 * is what catches that.
 *
 * <p>{@code sagaId} comes from the command that caused the transition; {@code userId},
 * {@code item} and {@code amount} come from the order itself. {@code item} is the
 * free-text description (section 2.1), because this is what gets rendered into a
 * customer-facing message — never {@code itemSku}.
 */
public record OrderConfirmedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String userId,
        String item,
        BigDecimal amount,
        Instant occurredAt) {

    public static OrderConfirmedEvent from(
            UUID sagaId, UUID orderId, String userId, String item, BigDecimal amount) {
        return new OrderConfirmedEvent(
                UUID.randomUUID(), sagaId, orderId, userId, item, amount, Instant.now());
    }
}
