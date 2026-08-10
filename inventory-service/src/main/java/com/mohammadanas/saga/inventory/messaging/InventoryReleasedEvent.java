package com.mohammadanas.saga.inventory.messaging;

import java.time.Instant;
import java.util.UUID;

/**
 * Compensation is confirmed. This is what lets the orchestrator move a saga from
 * {@code COMPENSATING} to {@code CANCELLED} (ARCHITECTURE.md section 3.3).
 */
public record InventoryReleasedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String item,
        int quantity,
        Instant occurredAt) {

    public static InventoryReleasedEvent from(UUID sagaId, UUID orderId, String item, int quantity) {
        return new InventoryReleasedEvent(UUID.randomUUID(), sagaId, orderId, item, quantity, Instant.now());
    }
}
