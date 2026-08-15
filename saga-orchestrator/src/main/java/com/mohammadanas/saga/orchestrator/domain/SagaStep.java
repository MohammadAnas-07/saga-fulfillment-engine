package com.mohammadanas.saga.orchestrator.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * One entry in a saga's history: an event consumed or a command issued.
 *
 * <p>Append-only and never read by the state machine — this table exists purely so a
 * finished saga can be explained after the fact. "Why did this order cancel?" is answered
 * by reading these rows in order, which is otherwise a log-grepping exercise across five
 * services.
 */
@Entity
@Table(name = "saga_steps", indexes = @Index(name = "idx_saga_steps_saga_id", columnList = "saga_id"))
public class SagaStep {

    public enum Direction {
        /** An event the orchestrator consumed. */
        INBOUND,
        /** A command or event the orchestrator published. */
        OUTBOUND,
        /** A state transition the orchestrator decided on. */
        TRANSITION
    }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "saga_id", nullable = false, updatable = false)
    private UUID sagaId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false, length = 16)
    private Direction direction;

    /** Message or transition name, e.g. {@code InventoryReserved} or {@code STARTED -> AWAITING_INVENTORY}. */
    @Column(nullable = false, updatable = false, length = 128)
    private String step;

    @Column(length = 512)
    private String detail;

    @CreationTimestamp
    @Column(name = "occurred_at", nullable = false, updatable = false)
    private Instant occurredAt;

    protected SagaStep() {
        // for JPA
    }

    private SagaStep(UUID sagaId, Direction direction, String step, String detail) {
        this.sagaId = sagaId;
        this.direction = direction;
        this.step = step;
        this.detail = detail;
    }

    public static SagaStep inbound(UUID sagaId, String step, String detail) {
        return new SagaStep(sagaId, Direction.INBOUND, step, detail);
    }

    public static SagaStep outbound(UUID sagaId, String step, String detail) {
        return new SagaStep(sagaId, Direction.OUTBOUND, step, detail);
    }

    public static SagaStep transition(UUID sagaId, SagaStatus from, SagaStatus to) {
        return new SagaStep(sagaId, Direction.TRANSITION, from + " -> " + to, null);
    }

    public Long getId() {
        return id;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public Direction getDirection() {
        return direction;
    }

    public String getStep() {
        return step;
    }

    public String getDetail() {
        return detail;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }
}
