package com.mohammadanas.saga.order.messaging;

import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes order-service's outbound events.
 *
 * <p>Messages are keyed so that everything about one saga lands on the same partition,
 * preserving per-saga ordering (ARCHITECTURE.md section 5.4). {@code OrderCreated} is the
 * documented exception: it is keyed by order id because it *precedes* saga creation and
 * there is no saga id to key by yet. The terminal-state events do have one, so they use
 * it, matching payment-service and inventory-service.
 */
@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishOrderCreated(OrderCreatedEvent event) {
        kafkaTemplate.send(OrderTopics.ORDER_CREATED, event.orderId().toString(), event);
        log.debug("Published OrderCreated for order {} (messageId={})", event.orderId(), event.messageId());
    }

    public void publishOrderConfirmed(OrderConfirmedEvent event) {
        kafkaTemplate.send(OrderTopics.ORDER_CONFIRMED, key(event.sagaId(), event.orderId()), event);
        log.debug("Published OrderConfirmed for order {} (saga {}, messageId={})",
                event.orderId(), event.sagaId(), event.messageId());
    }

    public void publishOrderCancelled(OrderCancelledEvent event) {
        kafkaTemplate.send(OrderTopics.ORDER_CANCELLED, key(event.sagaId(), event.orderId()), event);
        log.debug("Published OrderCancelled for order {} (saga {}, messageId={})",
                event.orderId(), event.sagaId(), event.messageId());
    }

    /**
     * Saga id is the partition key (section 5.4), falling back to order id if a command
     * ever arrives without one. The fallback keeps a related pair of messages together
     * rather than scattering null-keyed records round-robin across partitions; it is not
     * expected to trigger, since the orchestrator always populates {@code sagaId}.
     */
    private static String key(UUID sagaId, UUID orderId) {
        return (sagaId != null ? sagaId : orderId).toString();
    }
}
