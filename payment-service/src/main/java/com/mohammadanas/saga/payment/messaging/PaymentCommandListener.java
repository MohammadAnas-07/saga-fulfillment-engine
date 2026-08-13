package com.mohammadanas.saga.payment.messaging;

import com.mohammadanas.saga.payment.service.CommandOutcome;
import com.mohammadanas.saga.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Applies commands from saga-orchestrator.
 *
 * <p>Deliberately thin: deserialize, delegate, log. Any branching here would be saga
 * logic living in the wrong service.
 */
@Component
public class PaymentCommandListener {

    private static final Logger log = LoggerFactory.getLogger(PaymentCommandListener.class);

    private final PaymentService paymentService;

    public PaymentCommandListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = PaymentTopics.PROCESS_PAYMENT, groupId = "${spring.kafka.consumer.group-id}")
    public void onProcessPayment(ProcessPaymentCommand command) {
        CommandOutcome outcome = paymentService.processPayment(command);
        log.debug("ProcessPayment for order {} (saga {}) -> {}",
                command.orderId(), command.sagaId(), outcome);
    }

    @KafkaListener(topics = PaymentTopics.REFUND_PAYMENT, groupId = "${spring.kafka.consumer.group-id}")
    public void onRefundPayment(RefundPaymentCommand command) {
        CommandOutcome outcome = paymentService.refundPayment(command);
        log.debug("RefundPayment for order {} (saga {}) -> {}",
                command.orderId(), command.sagaId(), outcome);
    }
}
