package com.mohammadanas.saga.order.messaging;

import java.util.UUID;

/**
 * Command from saga-orchestrator telling order-service the saga failed, was compensated,
 * or timed out. Reached from several paths (ARCHITECTURE.md sections 3.2, 3.3, 4), which
 * is exactly why applying it must be idempotent.
 */
public record CancelOrderCommand(UUID messageId, UUID sagaId, UUID orderId) {
}
