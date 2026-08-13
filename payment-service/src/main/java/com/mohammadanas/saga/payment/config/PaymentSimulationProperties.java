package com.mohammadanas.saga.payment.config;

import java.math.BigDecimal;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Controls the simulated payment gateway.
 *
 * <h2>How to trigger a payment failure on purpose</h2>
 *
 * <p>A payment <strong>fails</strong> when its amount is <strong>strictly greater
 * than</strong> {@code failureThreshold} (default {@code 1000.00}); otherwise it
 * succeeds. The rule is deterministic on purpose — a random gateway would make the
 * compensation tests flaky, and the compensation path is the part of a saga worth
 * testing (ARCHITECTURE.md section 8.3).
 *
 * <p>So, to demo the failure path end to end, place an order whose <em>total</em> exceeds
 * the threshold. With the default, {@code unitPrice = 600.00, quantity = 2} gives
 * {@code amount = 1200.00} and reliably yields {@code PaymentFailed}. Note it is the
 * order total that matters, not the unit price.
 *
 * <p>Lower the threshold with {@code PAYMENT_FAILURE_THRESHOLD} (or
 * {@code payment.simulation.failure-threshold}) to make cheaper orders fail — setting it
 * to {@code 0} makes every payment fail, which is useful for exercising compensation
 * without crafting large orders.
 */
@ConfigurationProperties(prefix = "payment.simulation")
public class PaymentSimulationProperties {

    /** Amounts strictly above this fail. Inclusive boundary: exactly the threshold succeeds. */
    private BigDecimal failureThreshold = new BigDecimal("1000.00");

    public BigDecimal getFailureThreshold() {
        return failureThreshold;
    }

    public void setFailureThreshold(BigDecimal failureThreshold) {
        this.failureThreshold = failureThreshold;
    }
}
