package com.mohammadanas.saga.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/**
 * One order's hold on stock.
 *
 * <p>Recording the quantity here is what makes compensation possible: {@code
 * ReleaseInventory} identifies the reservation by order id alone, so the amount to hand
 * back must be recoverable from this row rather than from the command.
 */
@Entity
@Table(name = "reservations", indexes = @Index(name = "idx_reservations_order_id", columnList = "order_id"))
public class Reservation {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "saga_id", updatable = false)
    private UUID sagaId;

    @Column(name = "order_id", nullable = false, updatable = false)
    private UUID orderId;

    @Column(name = "item_id", nullable = false, updatable = false)
    private String itemId;

    @Column(nullable = false, updatable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private ReservationStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Reservation() {
        // for JPA
    }

    private Reservation(UUID id, UUID sagaId, UUID orderId, String itemId, int quantity, ReservationStatus status) {
        this.id = id;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.itemId = itemId;
        this.quantity = quantity;
        this.status = status;
    }

    public static Reservation reserved(UUID sagaId, UUID orderId, String itemId, int quantity) {
        return new Reservation(UUID.randomUUID(), sagaId, orderId, itemId, quantity, ReservationStatus.RESERVED);
    }

    public void markReleased() {
        this.status = ReservationStatus.RELEASED;
    }

    public UUID getId() {
        return id;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getItemId() {
        return itemId;
    }

    public int getQuantity() {
        return quantity;
    }

    public ReservationStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
