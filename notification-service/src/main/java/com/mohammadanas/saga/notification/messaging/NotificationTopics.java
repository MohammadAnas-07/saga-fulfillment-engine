package com.mohammadanas.saga.notification.messaging;

/**
 * Topics consumed by notification-service. It owns none — it is a pure consumer and
 * publishes nothing.
 *
 * <p>Names taken from ARCHITECTURE.md section 5.3. At the time this service was written
 * <strong>no producer existed for either topic</strong>: order-service publishes only
 * {@code OrderCreated}. Both are expected to be published by order-service once it emits
 * terminal-state events, which is why the record types here are the contract this service
 * asserts against rather than one it was handed.
 */
public final class NotificationTopics {

    public static final String ORDER_CONFIRMED = "order.events.order-confirmed.v1";
    public static final String ORDER_CANCELLED = "order.events.order-cancelled.v1";

    private NotificationTopics() {
    }
}
