package com.mohammadanas.saga.inventory.messaging;

import java.util.UUID;

/**
 * The compensating command: undo this order's reservation.
 *
 * <p>Carries no item or quantity — the reservation row is the record of what to hand
 * back, so a replayed or hand-crafted release cannot return the wrong amount.
 *
 * <p>Reachable from both an explicit payment failure and a scheduler timeout
 * (ARCHITECTURE.md sections 3.3 and 4), which is precisely why it must be idempotent.
 */
public record ReleaseInventoryCommand(
        UUID messageId,
        UUID sagaId,
        UUID orderId) {
}
