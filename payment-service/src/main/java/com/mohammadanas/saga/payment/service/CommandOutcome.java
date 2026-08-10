package com.mohammadanas.saga.payment.service;

/**
 * Result of handling a command, so tests can distinguish "did the work" from "correctly
 * declined to".
 */
public enum CommandOutcome {

    /** The payment was charged and a PaymentCompleted event published. */
    PAID,

    /** The simulated gateway declined; a PaymentFailed event was published. */
    PAYMENT_FAILED,

    /** A prior successful payment was reversed. */
    REFUNDED,

    /**
     * The command's messageId had already been processed. Nothing was charged and no
     * event published — the outcome that stops a redelivery becoming a second charge.
     */
    DUPLICATE_IGNORED,

    /** A refund arrived with no successful payment to reverse. Logged, no state change. */
    NO_REFUNDABLE_PAYMENT
}
