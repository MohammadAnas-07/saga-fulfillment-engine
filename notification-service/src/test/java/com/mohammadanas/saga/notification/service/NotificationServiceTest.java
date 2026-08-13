package com.mohammadanas.saga.notification.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.notification.domain.ProcessedMessage;
import com.mohammadanas.saga.notification.domain.ProcessedMessageRepository;
import com.mohammadanas.saga.notification.messaging.OrderCancelledEvent;
import com.mohammadanas.saga.notification.messaging.OrderConfirmedEvent;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Unit tests for notification rendering and delivery, with no infrastructure.
 *
 * <p>The duplicate case carries particular weight in this service. Elsewhere a replayed
 * message causes a duplicated state change, which is at least reversible; here it causes a
 * second message to a real customer, and a sent notification cannot be un-sent
 * (ARCHITECTURE.md section 2).
 */
@ExtendWith(MockitoExtension.class)
class NotificationServiceTest {

    private static final String USER_ID = "user-42";
    private static final String ITEM = "Mechanical keyboard";
    private static final BigDecimal AMOUNT = new BigDecimal("99.98");

    @Mock
    private ProcessedMessageRepository processedMessageRepository;

    @Mock
    private NotificationSender sender;

    @InjectMocks
    private NotificationService notificationService;

    private static OrderConfirmedEvent confirmedEvent() {
        return new OrderConfirmedEvent(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), USER_ID, ITEM, AMOUNT, Instant.now());
    }

    private static OrderCancelledEvent cancelledEvent() {
        return new OrderCancelledEvent(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), USER_ID, ITEM, AMOUNT, Instant.now());
    }

    @Nested
    @DisplayName("OrderConfirmed")
    class Confirmed {

        @Test
        @DisplayName("is consumed and produces a confirmation notification")
        void sendsConfirmation() {
            OrderConfirmedEvent event = confirmedEvent();
            when(processedMessageRepository.existsById(event.messageId())).thenReturn(false);

            NotificationOutcome outcome = notificationService.onOrderConfirmed(event);

            assertThat(outcome).isEqualTo(NotificationOutcome.SENT);

            ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
            verify(sender).send(captor.capture());

            Notification notification = captor.getValue();
            assertThat(notification.type()).isEqualTo(NotificationType.ORDER_CONFIRMED);
            assertThat(notification.orderId()).isEqualTo(event.orderId());
            assertThat(notification.userId()).isEqualTo(USER_ID);
            assertThat(notification.message())
                    .isEqualTo("Your order for Mechanical keyboard (99.98) is confirmed.");
        }
    }

    @Nested
    @DisplayName("OrderCancelled")
    class Cancelled {

        @Test
        @DisplayName("is consumed and produces a cancellation notification")
        void sendsCancellation() {
            OrderCancelledEvent event = cancelledEvent();
            when(processedMessageRepository.existsById(event.messageId())).thenReturn(false);

            NotificationOutcome outcome = notificationService.onOrderCancelled(event);

            assertThat(outcome).isEqualTo(NotificationOutcome.SENT);

            ArgumentCaptor<Notification> captor = ArgumentCaptor.forClass(Notification.class);
            verify(sender).send(captor.capture());

            Notification notification = captor.getValue();
            assertThat(notification.type()).isEqualTo(NotificationType.ORDER_CANCELLED);
            assertThat(notification.orderId()).isEqualTo(event.orderId());
            assertThat(notification.message())
                    .isEqualTo("Your order for Mechanical keyboard (99.98) was cancelled. "
                            + "You have not been charged.");
        }

        @Test
        @DisplayName("renders a different message from a confirmation, not a generic one")
        void cancellationDiffersFromConfirmation() {
            assertThat(NotificationService.cancelledMessage(ITEM, AMOUNT))
                    .isNotEqualTo(NotificationService.confirmedMessage(ITEM, AMOUNT));
        }
    }

    @Nested
    @DisplayName("idempotency — a redelivered event must not notify twice")
    class Idempotency {

        @Test
        @DisplayName("a redelivered OrderConfirmed sends nothing")
        void redeliveredConfirmedIsIgnored() {
            OrderConfirmedEvent event = confirmedEvent();
            when(processedMessageRepository.existsById(event.messageId())).thenReturn(true);

            NotificationOutcome outcome = notificationService.onOrderConfirmed(event);

            assertThat(outcome).isEqualTo(NotificationOutcome.DUPLICATE_IGNORED);
            verifyNoInteractions(sender);
            verify(processedMessageRepository, never()).save(any(ProcessedMessage.class));
        }

        @Test
        @DisplayName("a redelivered OrderCancelled sends nothing")
        void redeliveredCancelledIsIgnored() {
            OrderCancelledEvent event = cancelledEvent();
            when(processedMessageRepository.existsById(event.messageId())).thenReturn(true);

            assertThat(notificationService.onOrderCancelled(event))
                    .isEqualTo(NotificationOutcome.DUPLICATE_IGNORED);
            verifyNoInteractions(sender);
        }

        @Test
        @DisplayName("delivering the same OrderConfirmed twice notifies the customer exactly once")
        void sameEventTwiceNotifiesOnce() {
            OrderConfirmedEvent event = confirmedEvent();

            // First delivery: unseen. Second: the marker the first one wrote.
            when(processedMessageRepository.existsById(event.messageId()))
                    .thenReturn(false)
                    .thenReturn(true);

            NotificationOutcome first = notificationService.onOrderConfirmed(event);
            NotificationOutcome second = notificationService.onOrderConfirmed(event);

            assertThat(first).isEqualTo(NotificationOutcome.SENT);
            assertThat(second).isEqualTo(NotificationOutcome.DUPLICATE_IGNORED);

            // One message to the customer, never two — the point of this service's guard.
            verify(sender, times(1)).send(any(Notification.class));
        }

        @Test
        @DisplayName("the dedup marker records the event type it was written for")
        void marksMessageProcessed() {
            OrderCancelledEvent event = cancelledEvent();
            when(processedMessageRepository.existsById(event.messageId())).thenReturn(false);

            notificationService.onOrderCancelled(event);

            ArgumentCaptor<ProcessedMessage> captor = ArgumentCaptor.forClass(ProcessedMessage.class);
            verify(processedMessageRepository).save(captor.capture());
            assertThat(captor.getValue().getMessageId()).isEqualTo(event.messageId());
            assertThat(captor.getValue().getEventType()).isEqualTo("OrderCancelled");
        }

        @Test
        @DisplayName("two different events for the same order are both notified — dedup is per message, not per order")
        void differentMessagesForSameOrderBothNotify() {
            UUID orderId = UUID.randomUUID();
            OrderConfirmedEvent first = new OrderConfirmedEvent(
                    UUID.randomUUID(), UUID.randomUUID(), orderId, USER_ID, ITEM, AMOUNT, Instant.now());
            OrderConfirmedEvent second = new OrderConfirmedEvent(
                    UUID.randomUUID(), UUID.randomUUID(), orderId, USER_ID, ITEM, AMOUNT, Instant.now());

            when(processedMessageRepository.existsById(first.messageId())).thenReturn(false);
            when(processedMessageRepository.existsById(second.messageId())).thenReturn(false);

            assertThat(notificationService.onOrderConfirmed(first)).isEqualTo(NotificationOutcome.SENT);
            assertThat(notificationService.onOrderConfirmed(second)).isEqualTo(NotificationOutcome.SENT);

            verify(sender, times(2)).send(any(Notification.class));
        }
    }
}
