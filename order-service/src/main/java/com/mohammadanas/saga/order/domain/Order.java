package com.mohammadanas.saga.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * The order aggregate. order-service is the source of truth for order data, and
 * deliberately holds no saga state — the orchestrator owns that.
 */
@Entity
@Table(name = "orders")
public class Order {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "user_id", nullable = false, updatable = false)
    private String userId;

    /**
     * The inventory item this order is for, in the same identifier space as
     * inventory-service's {@code itemId}. This is what {@code ReserveInventory} carries;
     * {@link #item} is display text and is never used to look up stock.
     */
    @Column(name = "item_sku", nullable = false, updatable = false)
    private String itemSku;

    /** Free-text description for display. Not an inventory key. */
    @Column(nullable = false, updatable = false)
    private String item;

    @Column(nullable = false, updatable = false)
    private int quantity;

    @Column(nullable = false, updatable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private OrderStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {
        // for JPA
    }

    private Order(UUID id, String userId, String itemSku, String item, int quantity,
            BigDecimal amount, OrderStatus status) {
        this.id = id;
        this.userId = userId;
        this.itemSku = itemSku;
        this.item = item;
        this.quantity = quantity;
        this.amount = amount;
        this.status = status;
    }

    /** Creates a new order in {@link OrderStatus#PENDING}, the only valid entry state. */
    public static Order create(String userId, String itemSku, String item, int quantity, BigDecimal amount) {
        if (quantity < 1) {
            throw new IllegalArgumentException("quantity must be at least 1, was " + quantity);
        }
        return new Order(UUID.randomUUID(), userId, itemSku, item, quantity, amount, OrderStatus.PENDING);
    }

    /**
     * Moves this order to a terminal status.
     *
     * <p>Callers are responsible for checking {@link #isTerminal()} first; this method
     * performs the mutation unconditionally so that the decision (and its logging) stays
     * in one place in the service layer.
     */
    public void transitionTo(OrderStatus target) {
        this.status = target;
    }

    public boolean isTerminal() {
        return status.isTerminal();
    }

    public UUID getId() {
        return id;
    }

    public String getUserId() {
        return userId;
    }

    public String getItemSku() {
        return itemSku;
    }

    public String getItem() {
        return item;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
