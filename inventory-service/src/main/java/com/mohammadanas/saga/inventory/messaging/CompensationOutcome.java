package com.mohammadanas.saga.inventory.messaging;

/**
 * Whether a compensating action actually had something to undo.
 *
 * <p>Both values mean the same thing operationally — <em>compensation for this order is
 * complete, stop waiting</em> — and the orchestrator treats them identically when
 * deciding to leave {@code COMPENSATING}. The distinction exists so the audit trail can
 * still tell the two apart: a saga that released real stock and a saga that had nothing
 * reserved are equally finished, but they are not the same story.
 *
 * <p>payment-service declares an identically named and valued enum, so a consumer can
 * handle both confirmations the same way.
 */
public enum CompensationOutcome {

    /** There was something to undo, and it was undone. */
    REVERSED,

    /**
     * There was nothing to undo. Not an error: the saga may have failed before the
     * reservation was made, or compensation may have already run.
     */
    NOTHING_TO_REVERSE
}
