package com.mohammadanas.saga.inventory.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Compensation is confirmed. This is what lets the orchestrator move a saga from
 * {@code COMPENSATING} to {@code CANCELLED} (ARCHITECTURE.md section 3.3).
 *
 * <p>Published on <strong>every</strong> terminal outcome of the release handler,
 * including the case where there was no active reservation to reverse. Staying silent
 * there would leave the orchestrator waiting forever on a saga that simply never
 * reserved anything — a legitimate and common situation, not a failure.
 *
 * <p>The one deliberate exception is a redelivered {@code messageId}: that is not a new
 * outcome, it is the same outcome arriving twice, which is precisely what idempotency
 * exists to suppress.
 *
 * @param item     the released item, or {@code null} when {@code outcome} is
 *                 {@code NOTHING_TO_REVERSE} — with no reservation there is no item to name.
 * @param quantity units returned to stock; {@code 0} when there was nothing to reverse.
 */
public record InventoryReleasedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String item,
        int quantity,
        CompensationOutcome outcome,
        Instant occurredAt) {

    /** Confirmation that real stock was handed back. */
    public static InventoryReleasedEvent reversed(UUID sagaId, UUID orderId, String item, int quantity) {
        return new InventoryReleasedEvent(
                UUID.randomUUID(), sagaId, orderId, item, quantity,
                CompensationOutcome.REVERSED, Instant.now());
    }

    /** Confirmation that compensation is complete because there was nothing to undo. */
    public static InventoryReleasedEvent nothingToReverse(UUID sagaId, UUID orderId) {
        return new InventoryReleasedEvent(
                UUID.randomUUID(), sagaId, orderId, null, 0,
                CompensationOutcome.NOTHING_TO_REVERSE, Instant.now());
    }
}
