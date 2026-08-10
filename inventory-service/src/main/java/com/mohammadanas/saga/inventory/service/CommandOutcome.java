package com.mohammadanas.saga.inventory.service;

/**
 * Result of handling a command, so tests can distinguish "did the work" from "correctly
 * declined to".
 */
public enum CommandOutcome {

    /** Stock was reserved and an InventoryReserved event published. */
    RESERVED,

    /** Stock could not be reserved; an InventoryReservationFailed event was published. */
    RESERVATION_FAILED,

    /** A prior reservation was reversed and an InventoryReleased event published. */
    RELEASED,

    /**
     * The command's messageId had already been processed. No stock was touched and no
     * event was published — the defining idempotency outcome.
     */
    DUPLICATE_IGNORED,

    /** A release arrived with no active reservation to reverse. Logged, no stock change. */
    NO_ACTIVE_RESERVATION
}
