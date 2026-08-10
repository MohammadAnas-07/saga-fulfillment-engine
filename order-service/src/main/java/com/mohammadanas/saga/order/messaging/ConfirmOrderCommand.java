package com.mohammadanas.saga.order.messaging;

import java.util.UUID;

/**
 * Command from saga-orchestrator telling order-service the saga confirmed.
 *
 * <p>order-service does not interpret this — it applies it. All saga reasoning lives in
 * the orchestrator (ARCHITECTURE.md section 5.1).
 */
public record ConfirmOrderCommand(UUID messageId, UUID sagaId, UUID orderId) {
}
