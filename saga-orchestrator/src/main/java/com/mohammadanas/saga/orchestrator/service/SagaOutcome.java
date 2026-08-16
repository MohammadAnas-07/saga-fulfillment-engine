package com.mohammadanas.saga.orchestrator.service;

/**
 * Result of handling an inbound event, so tests can distinguish "advanced the machine"
 * from "correctly declined to".
 */
public enum SagaOutcome {

    /** The saga moved forward: state changed and any resulting commands were published. */
    ADVANCED,

    /** A compensation was confirmed, but others are still outstanding. Stays COMPENSATING. */
    AWAITING_MORE_COMPENSATION,

    /**
     * The event is not valid from the saga's current status — a late or replayed reply.
     * Dropped, never applied. This is what stops a finished saga being resurrected.
     */
    IGNORED_INVALID_TRANSITION,

    /** An OrderCreated for an order that already has a saga. */
    IGNORED_DUPLICATE,

    /**
     * This exact {@code messageId} has already been handled — a plain redelivery (§6).
     *
     * <p>Distinct from {@link #IGNORED_INVALID_TRANSITION} on purpose. That one means "this
     * event does not fit where the saga is now", which is a statement about status; this
     * one means "I have already processed this message", which is a statement about
     * identity. The gap between them is exactly why the dedup table exists: a redelivery
     * arriving while the saga is *still* in the status the reply is valid from passes the
     * status check.
     */
    IGNORED_REDELIVERY,

    /** A reply naming a saga that does not exist. Logged, not thrown, so the consumer cannot spin. */
    UNKNOWN_SAGA,

    /**
     * A timeout sweep asked to compensate a saga whose compensation is already underway.
     *
     * <p>Not an error and not a rejection: {@code COMPENSATING} is non-terminal, so a saga
     * that sits there waiting on a confirmation stays visible to the sweep (§4) and will be
     * offered again on the next pass. Re-issuing the compensating commands would achieve
     * nothing except a second round of them, so the sweep is told the work is in hand.
     */
    ALREADY_COMPENSATING
}
