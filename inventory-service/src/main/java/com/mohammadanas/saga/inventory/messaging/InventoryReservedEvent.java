package com.mohammadanas.saga.inventory.messaging;

import java.time.Instant;
import java.util.UUID;

public record InventoryReservedEvent(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String item,
        int quantity,
        Instant occurredAt) {

    public static InventoryReservedEvent from(UUID sagaId, UUID orderId, String item, int quantity) {
        return new InventoryReservedEvent(UUID.randomUUID(), sagaId, orderId, item, quantity, Instant.now());
    }
}
