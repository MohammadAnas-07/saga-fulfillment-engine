package com.mohammadanas.saga.inventory.messaging;

import java.util.UUID;

/**
 * Command from saga-orchestrator: hold {@code quantity} of {@code item} for this order.
 *
 * <p>{@code messageId} is the idempotency key — see
 * {@link com.mohammadanas.saga.inventory.domain.ProcessedCommand}.
 */
public record ReserveInventoryCommand(
        UUID messageId,
        UUID sagaId,
        UUID orderId,
        String item,
        int quantity) {
}
