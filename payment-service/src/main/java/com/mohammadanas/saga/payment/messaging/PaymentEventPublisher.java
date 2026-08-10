package com.mohammadanas.saga.payment.messaging;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes payment-service's result events.
 *
 * <p>Keyed by saga id so everything about one saga stays on the same partition and in
 * order (ARCHITECTURE.md section 5.4).
 */
@Component
public class PaymentEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishPaymentCompleted(PaymentCompletedEvent event) {
        kafkaTemplate.send(PaymentTopics.PAYMENT_COMPLETED, key(event.sagaId()), event);
        log.debug("Published PaymentCompleted for order {} (messageId={})", event.orderId(), event.messageId());
    }

    public void publishPaymentFailed(PaymentFailedEvent event) {
        kafkaTemplate.send(PaymentTopics.PAYMENT_FAILED, key(event.sagaId()), event);
        log.debug("Published PaymentFailed({}) for order {} (messageId={})",
                event.reason(), event.orderId(), event.messageId());
    }

    private static String key(UUID sagaId) {
        return sagaId == null ? null : sagaId.toString();
    }
}
