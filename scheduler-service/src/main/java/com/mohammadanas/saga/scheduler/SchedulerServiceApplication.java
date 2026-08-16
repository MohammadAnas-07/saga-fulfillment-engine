package com.mohammadanas.saga.scheduler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * The liveness watchdog (ARCHITECTURE.md section 4).
 *
 * <p>Sweeps for sagas past their timeout deadline, takes a Redis lock per saga, and asks
 * saga-orchestrator to compensate. Holds no saga state and no compensation logic of its
 * own — it decides <em>when</em>, the orchestrator decides <em>what</em>.
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class SchedulerServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(SchedulerServiceApplication.class, args);
    }
}
