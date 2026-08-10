package com.mohammadanas.saga.order.service;

/**
 * Result of applying an orchestrator command, so callers and tests can distinguish
 * "did the work" from "correctly declined to".
 *
 * <p>Only {@link #APPLIED} mutates the order. Every other outcome is a logged no-op —
 * Kafka is at-least-once, so redelivery is normal operation rather than an error
 * (ARCHITECTURE.md section 6).
 */
public enum CommandOutcome {

    /** The order was PENDING and moved to the requested terminal status. */
    APPLIED,

    /** The order already held the requested status. Redundant redelivery — ignored. */
    DUPLICATE_IGNORED,

    /**
     * The order is already terminal in the <em>other</em> status. Ignored rather than
     * applied, because a terminal order must never flip.
     */
    CONFLICT_IGNORED,

    /** No such order. Ignored rather than thrown, so the consumer does not spin. */
    ORDER_NOT_FOUND
}
