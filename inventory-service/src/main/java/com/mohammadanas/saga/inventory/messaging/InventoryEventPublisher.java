package com.mohammadanas.saga.inventory.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes inventory-service's result events.
 *
 * <p>Keyed by saga id so everything about one saga lands on the same partition and stays
 * ordered (ARCHITECTURE.md section 5.4).
 */
@Component
public class InventoryEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(InventoryEventPublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryEventPublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishInventoryReserved(InventoryReservedEvent event) {
        kafkaTemplate.send(InventoryTopics.INVENTORY_RESERVED, key(event.sagaId()), event);
        log.debug("Published InventoryReserved for order {} (messageId={})", event.orderId(), event.messageId());
    }

    public void publishInventoryReservationFailed(InventoryReservationFailedEvent event) {
        kafkaTemplate.send(InventoryTopics.INVENTORY_RESERVATION_FAILED, key(event.sagaId()), event);
        log.debug("Published InventoryReservationFailed({}) for order {} (messageId={})",
                event.reason(), event.orderId(), event.messageId());
    }

    public void publishInventoryReleased(InventoryReleasedEvent event) {
        kafkaTemplate.send(InventoryTopics.INVENTORY_RELEASED, key(event.sagaId()), event);
        log.debug("Published InventoryReleased for order {} (messageId={})", event.orderId(), event.messageId());
    }

    private static String key(java.util.UUID sagaId) {
        return sagaId == null ? null : sagaId.toString();
    }
}
