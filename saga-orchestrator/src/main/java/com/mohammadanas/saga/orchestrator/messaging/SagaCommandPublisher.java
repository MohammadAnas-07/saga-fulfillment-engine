package com.mohammadanas.saga.orchestrator.messaging;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes every command and terminal-state event the orchestrator issues.
 *
 * <p>All messages are keyed by saga id, so everything about one saga lands on one
 * partition and stays ordered — the only ordering guarantee the state machine needs
 * (§5.4).
 */
@Component
public class SagaCommandPublisher {

    private static final Logger log = LoggerFactory.getLogger(SagaCommandPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public SagaCommandPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void reserveInventory(OutboundMessages.ReserveInventory command) {
        send(SagaTopics.RESERVE_INVENTORY, command.sagaId(), command, "ReserveInventory");
    }

    public void releaseInventory(OutboundMessages.ReleaseInventory command) {
        send(SagaTopics.RELEASE_INVENTORY, command.sagaId(), command, "ReleaseInventory");
    }

    public void processPayment(OutboundMessages.ProcessPayment command) {
        send(SagaTopics.PROCESS_PAYMENT, command.sagaId(), command, "ProcessPayment");
    }

    public void refundPayment(OutboundMessages.RefundPayment command) {
        send(SagaTopics.REFUND_PAYMENT, command.sagaId(), command, "RefundPayment");
    }

    public void confirmOrder(OutboundMessages.ConfirmOrder command) {
        send(SagaTopics.CONFIRM_ORDER, command.sagaId(), command, "ConfirmOrder");
    }

    public void cancelOrder(OutboundMessages.CancelOrder command) {
        send(SagaTopics.CANCEL_ORDER, command.sagaId(), command, "CancelOrder");
    }

    private void send(String topic, UUID sagaId, Object payload, String name) {
        kafkaTemplate.send(topic, sagaId.toString(), payload);
        log.debug("Published {} for saga {}", name, sagaId);
    }
}
