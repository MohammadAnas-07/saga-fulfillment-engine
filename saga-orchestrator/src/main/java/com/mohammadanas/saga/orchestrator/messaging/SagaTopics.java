package com.mohammadanas.saga.orchestrator.messaging;

/**
 * Every topic the orchestrator touches, following
 * {@code <context>.<kind>.<name>.v<n>} from ARCHITECTURE.md section 5.2.
 *
 * <p>The orchestrator is the only publisher of commands and the only consumer of result
 * events — that asymmetry is orchestration rather than choreography (§5.1).
 */
public final class SagaTopics {

    // Consumed: the trigger and every service reply.
    public static final String ORDER_CREATED = "order.events.order-created.v1";
    public static final String INVENTORY_RESERVED = "inventory.events.inventory-reserved.v1";
    public static final String INVENTORY_RESERVATION_FAILED = "inventory.events.inventory-reservation-failed.v1";
    public static final String INVENTORY_RELEASED = "inventory.events.inventory-released.v1";
    public static final String PAYMENT_COMPLETED = "payment.events.payment-completed.v1";
    public static final String PAYMENT_FAILED = "payment.events.payment-failed.v1";
    public static final String PAYMENT_REFUNDED = "payment.events.payment-refunded.v1";

    // Published: commands addressed to exactly one executor each.
    public static final String RESERVE_INVENTORY = "inventory.commands.reserve-inventory.v1";
    public static final String RELEASE_INVENTORY = "inventory.commands.release-inventory.v1";
    public static final String PROCESS_PAYMENT = "payment.commands.process-payment.v1";
    public static final String REFUND_PAYMENT = "payment.commands.refund-payment.v1";
    public static final String CONFIRM_ORDER = "order.commands.confirm-order.v1";
    public static final String CANCEL_ORDER = "order.commands.cancel-order.v1";

    // Deliberately absent: order.events.order-confirmed.v1 and order-cancelled.v1.
    // Those are facts about the order aggregate and are order-service's to publish (§3.1).
    // The orchestrator issues the command and stops there.

    private SagaTopics() {
    }
}
