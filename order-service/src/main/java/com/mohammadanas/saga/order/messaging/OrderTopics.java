package com.mohammadanas.saga.order.messaging;

/**
 * Topic names owned or consumed by order-service.
 *
 * <p>Naming follows {@code <context>.<kind>.<name>.v<n>} from ARCHITECTURE.md section 5.2.
 */
public final class OrderTopics {

    /** Event published when an order is created. Keyed by order id — no saga exists yet. */
    public static final String ORDER_CREATED = "order.events.order-created.v1";

    /**
     * Event published when the order reaches CONFIRMED. Consumed by notification-service.
     *
     * <p>These two strings must match notification-service's {@code NotificationTopics}
     * exactly — a typo here is a consumer that never receives anything, with nothing
     * failing anywhere to say so.
     */
    public static final String ORDER_CONFIRMED = "order.events.order-confirmed.v1";

    /** Event published when the order reaches CANCELLED. Consumed by notification-service. */
    public static final String ORDER_CANCELLED = "order.events.order-cancelled.v1";

    /** Command from saga-orchestrator: the saga succeeded, confirm the order. */
    public static final String CONFIRM_ORDER = "order.commands.confirm-order.v1";

    /** Command from saga-orchestrator: the saga failed or timed out, cancel the order. */
    public static final String CANCEL_ORDER = "order.commands.cancel-order.v1";

    private OrderTopics() {
    }
}
