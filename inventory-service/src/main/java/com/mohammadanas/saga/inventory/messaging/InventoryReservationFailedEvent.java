package com.mohammadanas.saga.inventory.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Reservation could not be made. Drives the saga down the no-compensation-needed
 * cancellation path (ARCHITECTURE.md section 3.2) — nothing was reserved, so there is
 * nothing to undo.
 */
public record InventoryReservationFailedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String item,
        int quantity,
        ReservationFailureReason reason,
        Instant occurredAt) {

    public static InventoryReservationFailedEvent from(
            UUID sagaId, UUID orderId, String item, int quantity, ReservationFailureReason reason) {
        return new InventoryReservationFailedEvent(
                UUID.randomUUID(), sagaId, orderId, item, quantity, reason, Instant.now());
    }
}
