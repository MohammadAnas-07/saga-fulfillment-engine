package com.mohammadanas.saga.orchestrator.domain;

/**
 * The saga state machine (ARCHITECTURE.md section 3.4).
 *
 * <p>The intermediate {@code AWAITING_*} states exist so a stalled saga names what it is
 * stalled on. Without them every stuck saga would read simply as {@code STARTED} and the
 * timeout sweep could not tell an unanswered reservation from an unanswered payment.
 */
public enum SagaStatus {

    /** Saga record created; the first command has not been issued yet. */
    STARTED,

    /** {@code ReserveInventory} issued; waiting for inventory-service to reply. */
    AWAITING_INVENTORY,

    /** {@code ProcessPayment} issued; waiting for payment-service to reply. */
    AWAITING_PAYMENT,

    /**
     * Compensating commands issued; waiting for every one of them to be confirmed. A saga
     * sitting here genuinely means an undo is still outstanding — that is the whole reason
     * this state is modelled rather than collapsed into {@link #CANCELLED}.
     */
    COMPENSATING,

    CONFIRMED,
    CANCELLED;

    /** Terminal sagas are never swept by the scheduler and never transition again. */
    public boolean isTerminal() {
        return this == CONFIRMED || this == CANCELLED;
    }
}
