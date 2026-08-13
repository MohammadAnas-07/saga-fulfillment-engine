package com.mohammadanas.saga.payment.domain;

/**
 * Outcome of a payment attempt.
 *
 * <p>There is no {@code PENDING}: the simulated gateway resolves synchronously inside the
 * command handler, so a payment row only ever exists in a settled state. A real gateway
 * would need one, along with a webhook to settle it.
 */
public enum PaymentStatus {

    SUCCEEDED,
    FAILED,

    /** A previously successful payment that compensation has since reversed. */
    REFUNDED
}
