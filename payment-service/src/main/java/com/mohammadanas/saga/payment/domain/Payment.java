package com.mohammadanas.saga.payment.domain;

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
 * A payment attempt against one order.
 *
 * <p>{@code amount} is the order total, which order-service derives as
 * {@code unitPrice * quantity} (ARCHITECTURE.md section 2.1). payment-service takes it as
 * given and never recomputes it.
 */
@Entity
@Table(name = "payments", indexes = @Index(name = "idx_payments_order_id", columnList = "order_id"))
public class Payment {

    @Id
    @Column(nullable = false, updatable = false)
    private UUID id;

    @Column(name = "saga_id", updatable = false)
    private UUID sagaId;

    @Column(name = "order_id", nullable = false, updatable = false)
    private UUID orderId;

    @Column(name = "user_id", updatable = false)
    private String userId;

    @Column(nullable = false, updatable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 16)
    private PaymentStatus status;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Payment() {
        // for JPA
    }

    private Payment(UUID id, UUID sagaId, UUID orderId, String userId, BigDecimal amount, PaymentStatus status) {
        this.id = id;
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.userId = userId;
        this.amount = amount;
        this.status = status;
    }

    public static Payment succeeded(UUID sagaId, UUID orderId, String userId, BigDecimal amount) {
        return new Payment(UUID.randomUUID(), sagaId, orderId, userId, amount, PaymentStatus.SUCCEEDED);
    }

    public static Payment failed(UUID sagaId, UUID orderId, String userId, BigDecimal amount) {
        return new Payment(UUID.randomUUID(), sagaId, orderId, userId, amount, PaymentStatus.FAILED);
    }

    /**
     * Reverses a successful payment. Only a {@code SUCCEEDED} payment can be refunded —
     * refunding a failure would invent money that was never taken.
     */
    public void refund() {
        if (status != PaymentStatus.SUCCEEDED) {
            throw new IllegalStateException(
                    "Cannot refund payment " + id + " in status " + status + "; only SUCCEEDED can be refunded");
        }
        this.status = PaymentStatus.REFUNDED;
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

    public String getUserId() {
        return userId;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public PaymentStatus getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
