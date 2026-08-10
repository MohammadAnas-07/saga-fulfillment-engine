package com.mohammadanas.saga.order.messaging;

/**
 * Topic names owned or consumed by order-service.
 *
 * <p>Naming follows {@code <context>.<kind>.<name>.v<n>} from ARCHITECTURE.md section 5.2.
 */
public final class OrderTopics {

    /** Event published when an order is created. Keyed by order id — no saga exists yet. */
    public static final String ORDER_CREATED = "order.events.order-created.v1";

    /** Command from saga-orchestrator: the saga succeeded, confirm the order. */
    public static final String CONFIRM_ORDER = "order.commands.confirm-order.v1";

    /** Command from saga-orchestrator: the saga failed or timed out, cancel the order. */
    public static final String CANCEL_ORDER = "order.commands.cancel-order.v1";

    private OrderTopics() {
    }
}
