package com.mohammadanas.saga.notification.domain;

import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedMessageRepository extends JpaRepository<ProcessedMessage, UUID> {
}
