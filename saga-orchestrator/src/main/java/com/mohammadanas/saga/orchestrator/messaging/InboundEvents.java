package com.mohammadanas.saga.orchestrator.messaging;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * The events the orchestrator consumes.
 *
 * <p>Grouped in one file because they are a single contract surface — the replies the
 * orchestrator reacts to — and reading them together is how the state machine is checked
 * against the services that produce them. Each mirrors the producing service's record;
 * the topic plus the declared type is the contract (§5.3).
 */
public final class InboundEvents {

    private InboundEvents() {
    }

    /** From order-service. Carries no sagaId — the saga does not exist yet (§5.4). */
    public record OrderCreated(
            UUID messageId,
            UUID orderId,
            String userId,
            String itemSku,
            String item,
            int quantity,
            BigDecimal unitPrice,
            BigDecimal amount,
            Instant occurredAt) {
    }

    public record InventoryReserved(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            String item,
            int quantity,
            Instant occurredAt) {
    }

    public record InventoryReservationFailed(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            String item,
            int quantity,
            String reason,
            Instant occurredAt) {
    }

    /**
     * Compensation confirmation. Always arrives once {@code ReleaseInventory} is issued,
     * including when there was nothing to release (§5.3.1), which is what makes waiting
     * for it safe rather than a deadlock.
     */
    public record InventoryReleased(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            String item,
            int quantity,
            String outcome,
            Instant occurredAt) {
    }

    public record PaymentCompleted(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            UUID paymentId,
            BigDecimal amount,
            Instant occurredAt) {
    }

    public record PaymentFailed(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            UUID paymentId,
            BigDecimal amount,
            String reason,
            Instant occurredAt) {
    }

    /** Compensation confirmation, unconditional for the same reason as InventoryReleased. */
    public record PaymentRefunded(
            UUID messageId,
            UUID sagaId,
            UUID orderId,
            UUID paymentId,
            BigDecimal amount,
            String outcome,
            Instant occurredAt) {
    }
}
