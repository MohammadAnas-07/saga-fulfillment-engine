package com.mohammadanas.saga.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * Stock level for one item.
 *
 * <p>Quantities are split so reserved units are neither available to other sagas nor yet
 * consumed: reserving moves units from {@code available} to {@code reserved}, and the
 * compensating release moves them straight back. Their sum is invariant across both
 * operations, which is what makes a release a true undo — and is the property the
 * compensation tests assert.
 */
@Entity
@Table(name = "inventory")
public class Inventory {

    @Id
    @Column(name = "item_id", nullable = false, updatable = false)
    private String itemId;

    @Column(name = "available_quantity", nullable = false)
    private int availableQuantity;

    @Column(name = "reserved_quantity", nullable = false)
    private int reservedQuantity;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Inventory() {
        // for JPA
    }

    public Inventory(String itemId, int availableQuantity) {
        this.itemId = itemId;
        this.availableQuantity = availableQuantity;
        this.reservedQuantity = 0;
    }

    public boolean canReserve(int quantity) {
        return availableQuantity >= quantity;
    }

    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException(
                    "Cannot reserve " + quantity + " of " + itemId + "; only " + availableQuantity + " available");
        }
        availableQuantity -= quantity;
        reservedQuantity += quantity;
    }

    /** The compensating action: hands reserved units back to available. */
    public void release(int quantity) {
        if (reservedQuantity < quantity) {
            throw new IllegalStateException(
                    "Cannot release " + quantity + " of " + itemId + "; only " + reservedQuantity + " reserved");
        }
        reservedQuantity -= quantity;
        availableQuantity += quantity;
    }

    public String getItemId() {
        return itemId;
    }

    public int getAvailableQuantity() {
        return availableQuantity;
    }

    public int getReservedQuantity() {
        return reservedQuantity;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
