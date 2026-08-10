package com.mohammadanas.saga.inventory.messaging;

import com.mohammadanas.saga.inventory.service.CommandOutcome;
import com.mohammadanas.saga.inventory.service.InventoryService;
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
public class InventoryCommandListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryCommandListener.class);

    private final InventoryService inventoryService;

    public InventoryCommandListener(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @KafkaListener(topics = InventoryTopics.RESERVE_INVENTORY, groupId = "${spring.kafka.consumer.group-id}")
    public void onReserveInventory(ReserveInventoryCommand command) {
        CommandOutcome outcome = inventoryService.reserve(command);
        log.debug("ReserveInventory for order {} (saga {}) -> {}",
                command.orderId(), command.sagaId(), outcome);
    }

    @KafkaListener(topics = InventoryTopics.RELEASE_INVENTORY, groupId = "${spring.kafka.consumer.group-id}")
    public void onReleaseInventory(ReleaseInventoryCommand command) {
        CommandOutcome outcome = inventoryService.release(command);
        log.debug("ReleaseInventory for order {} (saga {}) -> {}",
                command.orderId(), command.sagaId(), outcome);
    }
}
