package com.mohammadanas.saga.payment.messaging;

/**
 * Topics owned or consumed by payment-service, following
 * {@code <context>.<kind>.<name>.v<n>} from ARCHITECTURE.md section 5.2.
 */
public final class PaymentTopics {

    public static final String PROCESS_PAYMENT = "payment.commands.process-payment.v1";

    /** Compensating command. Added in Chunk 3 — see ARCHITECTURE.md section 5.3. */
    public static final String REFUND_PAYMENT = "payment.commands.refund-payment.v1";

    public static final String PAYMENT_COMPLETED = "payment.events.payment-completed.v1";
    public static final String PAYMENT_FAILED = "payment.events.payment-failed.v1";

    private PaymentTopics() {
    }
}
