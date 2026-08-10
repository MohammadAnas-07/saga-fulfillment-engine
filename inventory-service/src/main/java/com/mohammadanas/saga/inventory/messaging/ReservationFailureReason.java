package com.mohammadanas.saga.inventory.messaging;

/**
 * Why a reservation could not be made.
 *
 * <p>An enum rather than free text so the orchestrator can branch on it later without
 * parsing strings. Both values are non-retryable — they mean the saga should be
 * cancelled, not that the command should be redelivered.
 */
public enum ReservationFailureReason {

    /** No inventory row exists for the requested item. */
    UNKNOWN_ITEM,

    /** The item exists but does not have enough available units. */
    INSUFFICIENT_STOCK
}
