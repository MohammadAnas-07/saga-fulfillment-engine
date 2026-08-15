package com.mohammadanas.saga.orchestrator.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * One saga instance: the workflow state for a single order.
 *
 * <p>The order details are copied here at creation rather than fetched later. The
 * orchestrator issues commands at several points in time and must be able to build each
 * one from its own state — going back to order-service for them would be the synchronous
 * inter-service call §1 rules out.
 *
 * <p>The two {@code awaiting*} flags are what make {@code COMPENSATING} honest: they
 * record which compensating commands are still unconfirmed, so the saga leaves
 * {@code COMPENSATING} only when every undo it dispatched has been acknowledged.
 */
@Entity
@Table(name = "sagas", indexes = {
        @Index(name = "idx_sagas_order_id", columnList = "order_id"),
        @Index(name = "idx_sagas_sweep", columnList = "status, timeout_deadline")
})
public class Saga {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "order_id", nullable = false, updatable = false)
    private UUID orderId;

    @Column(name = "user_id", nullable = false, updatable = false)
    private String userId;

    /** Inventory identifier, forwarded into {@code ReserveInventory}. */
    @Column(name = "item_sku", nullable = false, updatable = false)
    private String itemSku;

    /** Display text, forwarded into the customer-facing terminal events. */
    @Column(nullable = false, updatable = false)
    private String item;

    @Column(nullable = false, updatable = false)
    private int quantity;

    /** Order total, forwarded into {@code ProcessPayment}. */
    @Column(nullable = false, updatable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 24)
    private SagaStatus status;

    @Column(name = "timeout_deadline", nullable = false)
    private Instant timeoutDeadline;

    @Column(name = "awaiting_inventory_release", nullable = false)
    private boolean awaitingInventoryRelease;

    @Column(name = "awaiting_payment_refund", nullable = false)
    private boolean awaitingPaymentRefund;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Saga() {
        // for JPA
    }

    private Saga(UUID id, UUID orderId, String userId, String itemSku, String item,
            int quantity, BigDecimal amount, Instant timeoutDeadline) {
        this.id = id;
        this.orderId = orderId;
        this.userId = userId;
        this.itemSku = itemSku;
        this.item = item;
        this.quantity = quantity;
        this.amount = amount;
        this.timeoutDeadline = timeoutDeadline;
        this.status = SagaStatus.STARTED;
    }

    public static Saga start(UUID orderId, String userId, String itemSku, String item,
            int quantity, BigDecimal amount, Instant timeoutDeadline) {
        return new Saga(UUID.randomUUID(), orderId, userId, itemSku, item, quantity, amount, timeoutDeadline);
    }

    public void transitionTo(SagaStatus target) {
        this.status = target;
    }

    /** Records that a compensating command was dispatched and is not yet confirmed. */
    public void awaitInventoryRelease() {
        this.awaitingInventoryRelease = true;
    }

    public void awaitPaymentRefund() {
        this.awaitingPaymentRefund = true;
    }

    public void inventoryReleaseConfirmed() {
        this.awaitingInventoryRelease = false;
    }

    public void paymentRefundConfirmed() {
        this.awaitingPaymentRefund = false;
    }

    /** True once every dispatched compensation has been acknowledged. */
    public boolean compensationComplete() {
        return !awaitingInventoryRelease && !awaitingPaymentRefund;
    }

    public UUID getId() {
        return id;
    }

    public UUID getOrderId() {
        return orderId;
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

    public SagaStatus getStatus() {
        return status;
    }

    public Instant getTimeoutDeadline() {
        return timeoutDeadline;
    }

    public boolean isAwaitingInventoryRelease() {
        return awaitingInventoryRelease;
    }

    public boolean isAwaitingPaymentRefund() {
        return awaitingPaymentRefund;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
