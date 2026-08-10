package com.mohammadanas.saga.order.messaging;

import com.mohammadanas.saga.order.service.CommandOutcome;
import com.mohammadanas.saga.order.service.OrderService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Applies terminal-outcome commands from saga-orchestrator.
 *
 * <p>Deliberately thin: deserialize, delegate, log. There is no branching here, because
 * any branching would be saga logic living in the wrong service.
 */
@Component
public class OrderCommandListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCommandListener.class);

    private final OrderService orderService;

    public OrderCommandListener(OrderService orderService) {
        this.orderService = orderService;
    }

    @KafkaListener(topics = OrderTopics.CONFIRM_ORDER, groupId = "${spring.kafka.consumer.group-id}")
    public void onConfirmOrder(ConfirmOrderCommand command) {
        CommandOutcome outcome = orderService.confirmOrder(command.orderId(), command.messageId());
        log.debug("ConfirmOrder for order {} (saga {}) -> {}",
                command.orderId(), command.sagaId(), outcome);
    }

    @KafkaListener(topics = OrderTopics.CANCEL_ORDER, groupId = "${spring.kafka.consumer.group-id}")
    public void onCancelOrder(CancelOrderCommand command) {
        CommandOutcome outcome = orderService.cancelOrder(command.orderId(), command.messageId());
        log.debug("CancelOrder for order {} (saga {}) -> {}",
                command.orderId(), command.sagaId(), outcome);
    }
}
