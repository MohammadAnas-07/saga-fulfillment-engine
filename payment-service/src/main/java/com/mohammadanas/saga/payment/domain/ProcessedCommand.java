package com.mohammadanas.saga.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.CreationTimestamp;

/**
 * Record that a command message has already been handled.
 *
 * <p>Same pattern as inventory-service, and for a sharper reason: Kafka delivery is
 * at-least-once, so a consumer that dies after charging but before committing its offset
 * will see the same {@code ProcessPayment} again. Without this table that replay charges
 * the customer twice (ARCHITECTURE.md section 6).
 *
 * <p>The primary key <em>is</em> the message id, so uniqueness is the database's
 * guarantee rather than the application's. The row is written in the same transaction as
 * the payment, so the marker and the charge commit together or not at all.
 */
@Entity
@Table(name = "processed_commands")
public class ProcessedCommand {

    @Id
    @Column(name = "message_id", nullable = false, updatable = false)
    private UUID messageId;

    @Column(name = "command_type", nullable = false, updatable = false, length = 64)
    private String commandType;

    @CreationTimestamp
    @Column(name = "processed_at", nullable = false, updatable = false)
    private Instant processedAt;

    protected ProcessedCommand() {
        // for JPA
    }

    public ProcessedCommand(UUID messageId, String commandType) {
        this.messageId = messageId;
        this.commandType = commandType;
    }

    public UUID getMessageId() {
        return messageId;
    }

    public String getCommandType() {
        return commandType;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }
}
