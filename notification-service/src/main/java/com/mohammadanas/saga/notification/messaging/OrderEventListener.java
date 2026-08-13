package com.mohammadanas.saga.notification.messaging;

import com.mohammadanas.saga.notification.service.NotificationOutcome;
import com.mohammadanas.saga.notification.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes terminal-state order events.
 *
 * <p>Deliberately thin: deserialize, delegate, log. notification-service publishes
 * nothing and decides nothing about the saga.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final NotificationService notificationService;

    public OrderEventListener(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @KafkaListener(topics = NotificationTopics.ORDER_CONFIRMED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderConfirmed(OrderConfirmedEvent event) {
        NotificationOutcome outcome = notificationService.onOrderConfirmed(event);
        log.debug("OrderConfirmed for order {} (saga {}) -> {}",
                event.orderId(), event.sagaId(), outcome);
    }

    @KafkaListener(topics = NotificationTopics.ORDER_CANCELLED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(OrderCancelledEvent event) {
        NotificationOutcome outcome = notificationService.onOrderCancelled(event);
        log.debug("OrderCancelled for order {} (saga {}) -> {}",
                event.orderId(), event.sagaId(), outcome);
    }
}
