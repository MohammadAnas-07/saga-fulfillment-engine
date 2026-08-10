package com.mohammadanas.saga.payment.messaging;

/**
 * Why a payment did not succeed.
 *
 * <p>An enum rather than free text so the orchestrator can branch on it later without
 * parsing strings.
 */
public enum PaymentFailureReason {

    /**
     * The simulated gateway declined because the amount exceeded the configured failure
     * threshold. This is the deterministic hook used to trigger the compensation path on
     * demand — see {@code PaymentSimulationProperties}.
     */
    AMOUNT_ABOVE_THRESHOLD
}
