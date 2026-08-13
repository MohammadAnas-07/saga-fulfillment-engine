package com.mohammadanas.saga.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * Record that an event has already been notified on.
 *
 * <p>Same pattern and same message-id primary key as inventory-service's and
 * payment-service's {@code processed_commands} (ARCHITECTURE.md section 6). Named
 * {@code processed_messages} because this service consumes <em>events</em>, not commands.
 *
 * <p>Idempotency matters here for a different reason than elsewhere. The other services
 * guard against duplicated state changes, which are at least reversible; a notification
 * that has been delivered <strong>cannot be un-sent</strong> (§2). Sending a customer two
 * "your order is confirmed" messages is the failure this table exists to prevent, and it
 * is the reason a purely in-memory guard would not do — that would resend everything after
 * a restart.
 */
@Entity
@Table(name = "processed_messages")
public class ProcessedMessage {

    @Id
    @Column(name = "message_id", nullable = false, updatable = false)
    private UUID messageId;

    @Column(name = "event_type", nullable = false, updatable = false, length = 64)
    private String eventType;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedMessage() {
        // for JPA
    }

    public ProcessedMessage(UUID messageId, String eventType) {
        this.messageId = messageId;
        this.eventType = eventType;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
