package com.mohammadanas.saga.orchestrator.messaging;

import com.mohammadanas.saga.orchestrator.service.SagaOrchestrator;
import com.mohammadanas.saga.orchestrator.service.SagaOutcome;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the trigger event and every service reply.
 *
 * <p>Deliberately thin: deserialize, delegate, log. All decision-making lives in
 * {@link SagaOrchestrator} so the workflow is readable in one place (§5.1).
 */
@Component
public class SagaEventListener {

    private static final Logger log = LoggerFactory.getLogger(SagaEventListener.class);

    private final SagaOrchestrator orchestrator;

    public SagaEventListener(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @KafkaListener(topics = SagaTopics.ORDER_CREATED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreated(InboundEvents.OrderCreated event) {
        SagaOutcome outcome = orchestrator.onOrderCreated(event);
        log.debug("OrderCreated for order {} -> {}", event.orderId(), outcome);
    }

    @KafkaListener(topics = SagaTopics.INVENTORY_RESERVED, groupId = "${spring.kafka.consumer.group-id}")
    public void onInventoryReserved(InboundEvents.InventoryReserved event) {
        SagaOutcome outcome = orchestrator.onInventoryReserved(event);
        log.debug("InventoryReserved for saga {} -> {}", event.sagaId(), outcome);
    }

    @KafkaListener(topics = SagaTopics.INVENTORY_RESERVATION_FAILED, groupId = "${spring.kafka.consumer.group-id}")
    public void onInventoryReservationFailed(InboundEvents.InventoryReservationFailed event) {
        SagaOutcome outcome = orchestrator.onInventoryReservationFailed(event);
        log.debug("InventoryReservationFailed for saga {} -> {}", event.sagaId(), outcome);
    }

    @KafkaListener(topics = SagaTopics.INVENTORY_RELEASED, groupId = "${spring.kafka.consumer.group-id}")
    public void onInventoryReleased(InboundEvents.InventoryReleased event) {
        SagaOutcome outcome = orchestrator.onInventoryReleased(event);
        log.debug("InventoryReleased for saga {} -> {}", event.sagaId(), outcome);
    }

    @KafkaListener(topics = SagaTopics.PAYMENT_COMPLETED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentCompleted(InboundEvents.PaymentCompleted event) {
        SagaOutcome outcome = orchestrator.onPaymentCompleted(event);
        log.debug("PaymentCompleted for saga {} -> {}", event.sagaId(), outcome);
    }

    @KafkaListener(topics = SagaTopics.PAYMENT_FAILED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentFailed(InboundEvents.PaymentFailed event) {
        SagaOutcome outcome = orchestrator.onPaymentFailed(event);
        log.debug("PaymentFailed for saga {} -> {}", event.sagaId(), outcome);
    }

    @KafkaListener(topics = SagaTopics.PAYMENT_REFUNDED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentRefunded(InboundEvents.PaymentRefunded event) {
        SagaOutcome outcome = orchestrator.onPaymentRefunded(event);
        log.debug("PaymentRefunded for saga {} -> {}", event.sagaId(), outcome);
    }
}
