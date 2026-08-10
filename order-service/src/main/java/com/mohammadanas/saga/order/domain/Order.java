package com.mohammadanas.saga.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.math.RoundingMode;
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

    /** Price of one unit, normalised to 2 decimal places. Always greater than zero. */
    @Column(name = "unit_price", nullable = false, updatable = false, precision = 19, scale = 2)
    private BigDecimal unitPrice;

    /**
     * The order total, and strictly {@code unitPrice * quantity}.
     *
     * <p>Derived in the constructor and never accepted from a caller, so it cannot drift
     * from the arithmetic it claims to represent. This is the figure payment-service
     * charges.
     */
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
            BigDecimal unitPrice, OrderStatus status) {
        this.id = id;
        this.userId = userId;
        this.itemSku = itemSku;
        this.item = item;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.amount = unitPrice.multiply(BigDecimal.valueOf(quantity));
        this.status = status;
    }

    /**
     * Creates a new order in {@link OrderStatus#PENDING}, the only valid entry state.
     *
     * <p>The total is <em>computed</em> here rather than supplied, which is the whole
     * point: there is no code path that can persist an amount inconsistent with
     * {@code unitPrice * quantity}.
     *
     * <p>{@code unitPrice} is normalised to 2 decimal places before the multiplication,
     * so the product is exact at money scale and the stored total equals the stored unit
     * price times the stored quantity with no rounding left over. A price that rounds
     * away to zero at that scale is rejected rather than silently made free.
     */
    public static Order create(String userId, String itemSku, String item, int quantity, BigDecimal unitPrice) {
        if (quantity < 1) {
            throw new IllegalArgumentException("quantity must be at least 1, was " + quantity);
        }
        if (unitPrice == null || unitPrice.signum() <= 0) {
            throw new IllegalArgumentException("unitPrice must be greater than zero, was " + unitPrice);
        }

        BigDecimal normalisedUnitPrice = unitPrice.setScale(2, RoundingMode.HALF_UP);
        if (normalisedUnitPrice.signum() <= 0) {
            throw new IllegalArgumentException(
                    "unitPrice rounds to zero at 2 decimal places, was " + unitPrice);
        }

        return new Order(UUID.randomUUID(), userId, itemSku, item, quantity,
                normalisedUnitPrice, OrderStatus.PENDING);
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

    public BigDecimal getUnitPrice() {
        return unitPrice;
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
