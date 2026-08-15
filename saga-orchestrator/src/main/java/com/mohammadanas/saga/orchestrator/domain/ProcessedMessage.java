package com.mohammadanas.saga.orchestrator.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * Record that an inbound message has already been handled by the state machine.
 *
 * <p>Same pattern and same message-id primary key as inventory-service's and
 * payment-service's {@code processed_commands} and notification-service's
 * {@code processed_messages} (ARCHITECTURE.md section 6). Named for messages rather than
 * commands because the orchestrator consumes <em>events</em>: the trigger and six replies.
 *
 * <p>The row is written in the same transaction as the state change it corresponds to, so
 * the two commit together or not at all. A marker that could commit without its transition
 * would permanently suppress a reply the saga never actually processed — and since the
 * orchestrator is the only component that advances the machine, that saga would sit stuck
 * until the section 4 sweep compensated it.
 *
 * <p><strong>Why this is needed on top of the state-machine guard.</strong> Rejecting a
 * reply that is invalid from the current status already stops the common duplicate, and it
 * remains the guard for genuinely late replies. But it is a check on <em>status</em>, not
 * on message identity, and there is a real gap between the two: two deliveries of the same
 * reply that both arrive while the saga is still in the status that reply is valid from.
 * The status check passes for both, and the machine advances twice — issuing the next
 * command twice. This table is what makes "have I seen this exact message" answerable.
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
