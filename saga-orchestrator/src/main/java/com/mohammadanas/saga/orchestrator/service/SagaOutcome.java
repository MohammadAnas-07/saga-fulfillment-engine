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

    /** A reply naming a saga that does not exist. Logged, not thrown, so the consumer cannot spin. */
    UNKNOWN_SAGA
}
