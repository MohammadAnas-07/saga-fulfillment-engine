package com.mohammadanas.saga.orchestrator.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Orchestrator tuning.
 *
 * <h2>Timeout</h2>
 *
 * <p>{@code timeout} is how long a saga may stay non-terminal before the scheduler sweep
 * is entitled to compensate it. Default is 5 minutes: long enough that a healthy saga
 * never trips it, short enough to demonstrate the timeout path without a long wait. Drop
 * it to seconds ({@code SAGA_TIMEOUT=PT10S}) to demo the stuck-saga path deliberately.
 *
 * <p>The deadline is stamped once, at saga creation, and never extended as the saga
 * progresses. It therefore bounds the <em>whole</em> saga rather than each individual
 * step, which is the property the sweep needs: a saga that is merely slow at every step
 * still eventually gets resolved instead of drifting indefinitely.
 */
@ConfigurationProperties(prefix = "saga")
public class SagaProperties {

    private Duration timeout = Duration.ofMinutes(5);

    /** Cap on how many stuck sagas one sweep returns, so a large backlog cannot be pulled in one request. */
    private int stuckSagaBatchSize = 100;

    public Duration getTimeout() {
        return timeout;
    }

    public void setTimeout(Duration timeout) {
        this.timeout = timeout;
    }

    public int getStuckSagaBatchSize() {
        return stuckSagaBatchSize;
    }

    public void setStuckSagaBatchSize(int stuckSagaBatchSize) {
        this.stuckSagaBatchSize = stuckSagaBatchSize;
    }
}
