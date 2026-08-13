package com.mohammadanas.saga.payment;

import com.mohammadanas.saga.payment.config.PaymentSimulationProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Entry point for payment-service.
 *
 * <p>Consumes ProcessPayment and RefundPayment from saga-orchestrator and reports the
 * outcome. Payment itself is simulated — see {@link PaymentSimulationProperties} for how
 * to trigger a failure deliberately.
 */
@SpringBootApplication
@EnableConfigurationProperties(PaymentSimulationProperties.class)
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
