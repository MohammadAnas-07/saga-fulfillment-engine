package com.mohammadanas.saga.inventory.messaging;

/**
 * Topics owned or consumed by inventory-service, following
 * {@code <context>.<kind>.<name>.v<n>} from ARCHITECTURE.md section 5.2.
 */
public final class InventoryTopics {

    public static final String RESERVE_INVENTORY = "inventory.commands.reserve-inventory.v1";
    public static final String RELEASE_INVENTORY = "inventory.commands.release-inventory.v1";

    public static final String INVENTORY_RESERVED = "inventory.events.inventory-reserved.v1";
    public static final String INVENTORY_RESERVATION_FAILED = "inventory.events.inventory-reservation-failed.v1";
    public static final String INVENTORY_RELEASED = "inventory.events.inventory-released.v1";

    private InventoryTopics() {
    }
}
