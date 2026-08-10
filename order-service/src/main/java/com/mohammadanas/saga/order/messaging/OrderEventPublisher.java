package com.mohammadanas.saga.order.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes order-service's outbound events.
 *
 * <p>Messages are keyed so that everything about one order lands on the same partition,
 * preserving per-saga ordering (ARCHITECTURE.md section 5.4).
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
}
