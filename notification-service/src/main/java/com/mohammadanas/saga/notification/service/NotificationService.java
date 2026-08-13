package com.mohammadanas.saga.notification.service;

import com.mohammadanas.saga.notification.domain.ProcessedMessage;
import com.mohammadanas.saga.notification.domain.ProcessedMessageRepository;
import com.mohammadanas.saga.notification.messaging.OrderCancelledEvent;
import com.mohammadanas.saga.notification.messaging.OrderConfirmedEvent;
import java.math.BigDecimal;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Renders and sends customer notifications for terminal order outcomes.
 *
 * <p>Runs only on terminal states, and deliberately has no compensating action: a sent
 * notification cannot be un-sent (ARCHITECTURE.md section 2). That one-way property is
 * why the dedup guard here is not optional.
 */
@Service
public class NotificationService {

    private static final String ORDER_CONFIRMED = "OrderConfirmed";
    private static final String ORDER_CANCELLED = "OrderCancelled";

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    private final ProcessedMessageRepository processedMessageRepository;
    private final NotificationSender sender;

    public NotificationService(
            ProcessedMessageRepository processedMessageRepository, NotificationSender sender) {
        this.processedMessageRepository = processedMessageRepository;
        this.sender = sender;
    }

    @Transactional
    public NotificationOutcome onOrderConfirmed(OrderConfirmedEvent event) {
        if (alreadyNotified(event.messageId(), ORDER_CONFIRMED, event.orderId())) {
            return NotificationOutcome.DUPLICATE_IGNORED;
        }

        Notification notification = new Notification(
                NotificationType.ORDER_CONFIRMED,
                event.orderId(),
                event.userId(),
                confirmedMessage(event.item(), event.amount()));

        return dispatch(notification, event.messageId(), ORDER_CONFIRMED);
    }

    @Transactional
    public NotificationOutcome onOrderCancelled(OrderCancelledEvent event) {
        if (alreadyNotified(event.messageId(), ORDER_CANCELLED, event.orderId())) {
            return NotificationOutcome.DUPLICATE_IGNORED;
        }

        Notification notification = new Notification(
                NotificationType.ORDER_CANCELLED,
                event.orderId(),
                event.userId(),
                cancelledMessage(event.item(), event.amount()));

        return dispatch(notification, event.messageId(), ORDER_CANCELLED);
    }

    static String confirmedMessage(String item, BigDecimal amount) {
        return "Your order for " + item + " (" + amount + ") is confirmed.";
    }

    static String cancelledMessage(String item, BigDecimal amount) {
        return "Your order for " + item + " (" + amount + ") was cancelled. You have not been charged.";
    }

    /**
     * Marks the message processed <em>before</em> handing it to the sender, so both commit
     * together. The ordering is deliberate: if delivery throws, the transaction rolls back
     * and the marker goes with it, leaving the event eligible for redelivery rather than
     * silently swallowed.
     */
    private NotificationOutcome dispatch(Notification notification, UUID messageId, String eventType) {
        processedMessageRepository.save(new ProcessedMessage(messageId, eventType));
        sender.send(notification);
        return NotificationOutcome.SENT;
    }

    private boolean alreadyNotified(UUID messageId, String eventType, UUID orderId) {
        if (processedMessageRepository.existsById(messageId)) {
            log.info("Ignoring redelivered {} (messageId={}) for order {}: already notified",
                    eventType, messageId, orderId);
            return true;
        }
        return false;
    }
}
