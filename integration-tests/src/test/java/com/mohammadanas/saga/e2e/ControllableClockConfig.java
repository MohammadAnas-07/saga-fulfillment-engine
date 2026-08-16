package com.mohammadanas.saga.e2e;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Supplies saga-orchestrator with the test's {@link MutableClock}.
 *
 * <p>Added as an extra primary source alongside {@code SagaOrchestratorApplication}, and
 * marked {@code @Primary} so it wins over the orchestrator's own {@code Clock} bean without
 * needing bean-definition overriding. The orchestrator injects {@code Clock} precisely so
 * this is possible — its own comment says the indirection exists so the sweep can be tested
 * against a controlled clock rather than a sleep.
 *
 * <p>This is the only substitution anywhere in the end-to-end suite, and it replaces a
 * clock, not a collaborator: every service, message, database and HTTP call remains real.
 */
@Configuration
public class ControllableClockConfig {

    @Bean
    @Primary
    public Clock controllableClock() {
        return SagaCluster.clock();
    }
}
