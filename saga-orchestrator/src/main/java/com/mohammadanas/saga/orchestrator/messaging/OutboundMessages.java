package com.mohammadanas.saga.orchestrator.messaging;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * The commands and events the orchestrator publishes.
 *
 * <p>Field names and shapes must match the consuming services' records exactly — the wire
 * contract is JSON resolved against the listener's declared type, so a renamed field is a
 * silently-null field on the other side rather than a compile error. They are grouped here
 * so that surface can be reviewed in one place against §5.3.
 */
public final class OutboundMessages {

    private OutboundMessages() {
    }

    /** {@code item} carries the inventory identifier — inventory-service matches it against itemId. */
    public record ReserveInventory(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            String item,
            int quantity) {
    }

    /** Compensating command. Carries no quantity: the reservation row is the record of what to return. */
    public record ReleaseInventory(
            UUID messageId,
            UUID sagaId,
            UUID orderId) {
    }

    public record ProcessPayment(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            String userId,
            BigDecimal amount) {
    }

    /** Compensating command. Carries no amount: the payment row is the record of what was taken. */
    public record RefundPayment(
            UUID messageId,
            UUID sagaId,
            UUID orderId) {
    }

    public record ConfirmOrder(
            UUID messageId,
            UUID sagaId,
            UUID orderId) {
    }

    public record CancelOrder(
            UUID messageId,
            UUID sagaId,
            UUID orderId) {
    }

    // No OrderConfirmed / OrderCancelled here: the terminal-state events belong to
    // order-service, which owns the order aggregate (§3.1). The orchestrator tells it to
    // confirm or cancel; announcing the resulting fact is not the orchestrator's to do.
}
