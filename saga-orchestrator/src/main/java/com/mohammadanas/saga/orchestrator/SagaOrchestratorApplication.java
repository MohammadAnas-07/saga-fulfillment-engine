package com.mohammadanas.saga.orchestrator;

import com.mohammadanas.saga.orchestrator.config.SagaProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for saga-orchestrator: the only component that decides what happens next in
 * a saga (ARCHITECTURE.md section 5.1).
 */
@SpringBootApplication
@EnableConfigurationProperties(SagaProperties.class)
public class SagaOrchestratorApplication {

    public static void main(String[] args) {
        SpringApplication.run(SagaOrchestratorApplication.class, args);
    }
}
