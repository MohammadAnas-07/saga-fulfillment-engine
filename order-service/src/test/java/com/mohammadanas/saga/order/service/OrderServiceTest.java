package com.mohammadanas.saga.order.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.order.domain.Order;
import com.mohammadanas.saga.order.domain.OrderRepository;
import com.mohammadanas.saga.order.domain.OrderStatus;
import com.mohammadanas.saga.order.messaging.CancelOrderCommand;
import com.mohammadanas.saga.order.messaging.ConfirmOrderCommand;
import com.mohammadanas.saga.order.messaging.OrderCancelledEvent;
import com.mohammadanas.saga.order.messaging.OrderConfirmedEvent;
import com.mohammadanas.saga.order.messaging.OrderCreatedEvent;
import com.mohammadanas.saga.order.messaging.OrderEventPublisher;
import java.math.BigDecimal;
import java.util.Optional;
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
 * Unit tests for the order lifecycle rules, with no infrastructure.
 *
 * <p>The idempotency cases are the point of this class: Kafka redelivery is normal
 * operation, so a repeated command must be a logged no-op rather than an error or a
 * second mutation (ARCHITECTURE.md section 6).
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    private static final String USER_ID = "user-1";
    private static final String ITEM_SKU = "MECH-KB-01";
    private static final String ITEM = "Mechanical keyboard";
    private static final int QUANTITY = 2;
    private static final BigDecimal UNIT_PRICE = new BigDecimal("49.99");
    /** Always UNIT_PRICE * QUANTITY — restated here so the test asserts the arithmetic, not the code's opinion of it. */
    private static final BigDecimal EXPECTED_TOTAL = new BigDecimal("99.98");

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OrderEventPublisher eventPublisher;

    @InjectMocks
    private OrderService orderService;

    private static Order orderIn(OrderStatus status) {
        Order order = Order.create(USER_ID, ITEM_SKU, ITEM, QUANTITY, UNIT_PRICE);
        if (status != OrderStatus.PENDING) {
            order.transitionTo(status);
        }
        return order;
    }

    private static ConfirmOrderCommand confirm(UUID orderId) {
        return new ConfirmOrderCommand(UUID.randomUUID(), UUID.randomUUID(), orderId);
    }

    private static CancelOrderCommand cancel(UUID orderId) {
        return new CancelOrderCommand(UUID.randomUUID(), UUID.randomUUID(), orderId);
    }

    @Nested
    @DisplayName("createOrder")
    class CreateOrder {

        @Test
        @DisplayName("persists the order as PENDING")
        void persistsAsPending() {
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            Order created = orderService.createOrder(USER_ID, ITEM_SKU, ITEM, QUANTITY, UNIT_PRICE);

            assertThat(created.getStatus()).isEqualTo(OrderStatus.PENDING);
            assertThat(created.getUserId()).isEqualTo(USER_ID);
            assertThat(created.getItemSku()).isEqualTo(ITEM_SKU);
            assertThat(created.getItem()).isEqualTo(ITEM);
            assertThat(created.getQuantity()).isEqualTo(QUANTITY);
            assertThat(created.getUnitPrice()).isEqualByComparingTo(UNIT_PRICE);
        }

        @Test
        @DisplayName("publishes OrderCreated carrying orderId, userId, itemSku, item, quantity, unitPrice and amount")
        void publishesOrderCreatedWithCorrectPayload() {
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            Order created = orderService.createOrder(USER_ID, ITEM_SKU, ITEM, QUANTITY, UNIT_PRICE);

            ArgumentCaptor<OrderCreatedEvent> captor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
            verify(eventPublisher).publishOrderCreated(captor.capture());

            OrderCreatedEvent event = captor.getValue();
            assertThat(event.orderId()).isEqualTo(created.getId());
            assertThat(event.userId()).isEqualTo(USER_ID);
            assertThat(event.item()).isEqualTo(ITEM);
            assertThat(event.messageId()).isNotNull();
            assertThat(event.occurredAt()).isNotNull();
            assertThat(event.itemSku()).isEqualTo(ITEM_SKU);
            assertThat(event.quantity()).isEqualTo(QUANTITY);

            // The event must carry the derived total, not the unit price, since
            // ProcessPayment charges `amount`.
            assertThat(event.unitPrice()).isEqualByComparingTo(UNIT_PRICE);
            assertThat(event.amount()).isEqualByComparingTo(EXPECTED_TOTAL);
        }

        @Test
        @DisplayName("rejects a quantity below 1 at the domain boundary, not just at the API")
        void rejectsNonPositiveQuantity() {
            org.assertj.core.api.Assertions
                    .assertThatThrownBy(() -> orderService.createOrder(USER_ID, ITEM_SKU, ITEM, 0, UNIT_PRICE))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("quantity must be at least 1");
        }
    }

    @Nested
    @DisplayName("amount is derived, never supplied")
    class TotalIsDerived {

        @Test
        @DisplayName("amount equals unitPrice * quantity")
        void computesTotal() {
            Order order = Order.create(USER_ID, ITEM_SKU, ITEM, QUANTITY, UNIT_PRICE);

            assertThat(order.getAmount()).isEqualByComparingTo(EXPECTED_TOTAL);
            assertThat(order.getAmount())
                    .isEqualByComparingTo(UNIT_PRICE.multiply(BigDecimal.valueOf(QUANTITY)));
        }

        @Test
        @DisplayName("a quantity of 1 makes the total equal the unit price")
        void singleUnitTotalEqualsUnitPrice() {
            Order order = Order.create(USER_ID, ITEM_SKU, ITEM, 1, new BigDecimal("15.00"));

            assertThat(order.getAmount()).isEqualByComparingTo(new BigDecimal("15.00"));
        }

        @Test
        @DisplayName("the total stays exact at money scale for a large quantity")
        void largeQuantityStaysExact() {
            Order order = Order.create(USER_ID, ITEM_SKU, ITEM, 1000, new BigDecimal("0.07"));

            assertThat(order.getAmount()).isEqualByComparingTo(new BigDecimal("70.00"));
        }

        @Test
        @DisplayName("a sub-cent unit price is normalised to 2dp before multiplying")
        void normalisesUnitPriceToMoneyScale() {
            Order order = Order.create(USER_ID, ITEM_SKU, ITEM, 3, new BigDecimal("1.005"));

            // 1.005 rounds HALF_UP to 1.01, so the total is 3.03 and not 3.015 —
            // the stored total always equals the stored unit price times quantity.
            assertThat(order.getUnitPrice()).isEqualByComparingTo(new BigDecimal("1.01"));
            assertThat(order.getAmount()).isEqualByComparingTo(new BigDecimal("3.03"));
        }

        @Test
        @DisplayName("rejects a non-positive unit price")
        void rejectsNonPositiveUnitPrice() {
            org.assertj.core.api.Assertions
                    .assertThatThrownBy(() -> Order.create(USER_ID, ITEM_SKU, ITEM, 1, BigDecimal.ZERO))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unitPrice must be greater than zero");
        }

        @Test
        @DisplayName("rejects a unit price that rounds away to zero rather than making it free")
        void rejectsUnitPriceRoundingToZero() {
            org.assertj.core.api.Assertions
                    .assertThatThrownBy(
                            () -> Order.create(USER_ID, ITEM_SKU, ITEM, 1, new BigDecimal("0.001")))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("rounds to zero");
        }
    }

    @Nested
    @DisplayName("ConfirmOrder / CancelOrder on a PENDING order")
    class AppliesTerminalStatus {

        @Test
        @DisplayName("ConfirmOrder sets CONFIRMED")
        void confirmSetsConfirmed() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            CommandOutcome outcome = orderService.confirmOrder(confirm(order.getId()));

            assertThat(outcome).isEqualTo(CommandOutcome.APPLIED);
            assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
            verify(orderRepository).save(order);
        }

        @Test
        @DisplayName("CancelOrder sets CANCELLED")
        void cancelSetsCancelled() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            CommandOutcome outcome = orderService.cancelOrder(cancel(order.getId()));

            assertThat(outcome).isEqualTo(CommandOutcome.APPLIED);
            assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
            verify(orderRepository).save(order);
        }
    }

    /**
     * The terminal-state events, and specifically the payload notification-service needs.
     *
     * <p>Before this chunk both topics had a complete consumer and no producer at all
     * (ARCHITECTURE.md section 5.3), so these assertions are the first thing anywhere that
     * checks the two halves agree. They assert the <em>values</em>; the JSON field names
     * they travel under are pinned separately by {@code TerminalEventContractTest}.
     */
    @Nested
    @DisplayName("terminal-state events")
    class PublishesTerminalStateEvents {

        @Test
        @DisplayName("ConfirmOrder publishes OrderConfirmed with everything notification-service renders")
        void confirmPublishesOrderConfirmed() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            ConfirmOrderCommand command = confirm(order.getId());
            orderService.confirmOrder(command);

            ArgumentCaptor<OrderConfirmedEvent> captor = ArgumentCaptor.forClass(OrderConfirmedEvent.class);
            verify(eventPublisher).publishOrderConfirmed(captor.capture());
            OrderConfirmedEvent event = captor.getValue();

            // sagaId can only come from the command — order-service holds no saga state.
            assertThat(event.sagaId()).isEqualTo(command.sagaId());
            assertThat(event.orderId()).isEqualTo(order.getId());

            // Who to tell, and what about. notification-service reads exactly these three.
            assertThat(event.userId()).isEqualTo(USER_ID);
            assertThat(event.item()).isEqualTo(ITEM);
            assertThat(event.amount()).isEqualByComparingTo(EXPECTED_TOTAL);

            // A fresh messageId, not the command's: this is a different message on a
            // different topic, and it is notification-service's dedup key.
            assertThat(event.messageId()).isNotNull().isNotEqualTo(command.messageId());
            assertThat(event.occurredAt()).isNotNull();
        }

        @Test
        @DisplayName("CancelOrder publishes OrderCancelled with everything notification-service renders")
        void cancelPublishesOrderCancelled() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            CancelOrderCommand command = cancel(order.getId());
            orderService.cancelOrder(command);

            ArgumentCaptor<OrderCancelledEvent> captor = ArgumentCaptor.forClass(OrderCancelledEvent.class);
            verify(eventPublisher).publishOrderCancelled(captor.capture());
            OrderCancelledEvent event = captor.getValue();

            assertThat(event.sagaId()).isEqualTo(command.sagaId());
            assertThat(event.orderId()).isEqualTo(order.getId());
            assertThat(event.userId()).isEqualTo(USER_ID);
            assertThat(event.item()).isEqualTo(ITEM);
            assertThat(event.amount()).isEqualByComparingTo(EXPECTED_TOTAL);
            assertThat(event.messageId()).isNotNull().isNotEqualTo(command.messageId());
            assertThat(event.occurredAt()).isNotNull();
        }

        @Test
        @DisplayName("the event carries the order total, not the unit price — it is what the customer is told")
        void publishesDerivedTotalNotUnitPrice() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            orderService.confirmOrder(confirm(order.getId()));

            ArgumentCaptor<OrderConfirmedEvent> captor = ArgumentCaptor.forClass(OrderConfirmedEvent.class);
            verify(eventPublisher).publishOrderConfirmed(captor.capture());

            assertThat(captor.getValue().amount())
                    .isEqualByComparingTo(EXPECTED_TOTAL)
                    .isNotEqualByComparingTo(UNIT_PRICE);
        }

        @Test
        @DisplayName("the event carries the display description, never the SKU")
        void publishesDisplayItemNotSku() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            orderService.cancelOrder(cancel(order.getId()));

            ArgumentCaptor<OrderCancelledEvent> captor = ArgumentCaptor.forClass(OrderCancelledEvent.class);
            verify(eventPublisher).publishOrderCancelled(captor.capture());

            // "Your order for Mechanical keyboard ..." reads to a customer;
            // "Your order for MECH-KB-01 ..." does not (ARCHITECTURE.md section 2.1).
            assertThat(captor.getValue().item()).isEqualTo(ITEM).isNotEqualTo(ITEM_SKU);
        }
    }

    @Nested
    @DisplayName("idempotency — a terminal order ignores a redundant command")
    class RedundantCommandsAreNoOps {

        @Test
        @DisplayName("ConfirmOrder on an already CONFIRMED order is a no-op, not an error")
        void confirmOnConfirmedIsNoOp() {
            Order order = orderIn(OrderStatus.CONFIRMED);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

            CommandOutcome outcome = orderService.confirmOrder(confirm(order.getId()));

            assertThat(outcome).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
            verify(orderRepository, never()).save(any(Order.class));
        }

        @Test
        @DisplayName("CancelOrder on an already CANCELLED order is a no-op, not an error")
        void cancelOnCancelledIsNoOp() {
            Order order = orderIn(OrderStatus.CANCELLED);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

            CommandOutcome outcome = orderService.cancelOrder(cancel(order.getId()));

            assertThat(outcome).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
            verify(orderRepository, never()).save(any(Order.class));
        }

        /**
         * The one that protects the customer. Kafka redelivery is normal operation, and a
         * re-published event would carry a fresh {@code messageId} — so it would slip past
         * notification-service's own dedup and send a second message. Suppressing it has
         * to happen here.
         */
        @Test
        @DisplayName("a redelivered ConfirmOrder does not publish OrderConfirmed a second time")
        void redeliveredConfirmDoesNotRepublish() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            ConfirmOrderCommand command = confirm(order.getId());

            assertThat(orderService.confirmOrder(command)).isEqualTo(CommandOutcome.APPLIED);
            // The exact same message again, as an at-least-once broker would redeliver it.
            assertThat(orderService.confirmOrder(command)).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);

            verify(eventPublisher, times(1)).publishOrderConfirmed(any(OrderConfirmedEvent.class));
        }

        @Test
        @DisplayName("a redelivered CancelOrder does not publish OrderCancelled a second time")
        void redeliveredCancelDoesNotRepublish() {
            Order order = orderIn(OrderStatus.PENDING);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
            when(orderRepository.save(any(Order.class))).thenAnswer(inv -> inv.getArgument(0));

            CancelOrderCommand command = cancel(order.getId());

            assertThat(orderService.cancelOrder(command)).isEqualTo(CommandOutcome.APPLIED);
            assertThat(orderService.cancelOrder(command)).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);

            verify(eventPublisher, times(1)).publishOrderCancelled(any(OrderCancelledEvent.class));
        }

        /**
         * A <em>different</em> command for the same already-terminal order, which is what a
         * timeout sweep re-issuing {@code CancelOrder} looks like. The order's status is
         * the idempotency key, not the message id, so this is suppressed too.
         */
        @Test
        @DisplayName("a fresh command against an already-terminal order publishes nothing")
        void freshCommandAgainstTerminalOrderPublishesNothing() {
            Order order = orderIn(OrderStatus.CANCELLED);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

            assertThat(orderService.cancelOrder(cancel(order.getId())))
                    .isEqualTo(CommandOutcome.DUPLICATE_IGNORED);

            verify(eventPublisher, never()).publishOrderCancelled(any(OrderCancelledEvent.class));
        }
    }

    @Nested
    @DisplayName("a terminal order never flips to the other terminal status")
    class ConflictingCommandsAreRejected {

        @Test
        @DisplayName("ConfirmOrder on a CANCELLED order leaves it CANCELLED and announces nothing")
        void confirmOnCancelledIsIgnored() {
            Order order = orderIn(OrderStatus.CANCELLED);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

            CommandOutcome outcome = orderService.confirmOrder(confirm(order.getId()));

            assertThat(outcome).isEqualTo(CommandOutcome.CONFLICT_IGNORED);
            assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
            verify(orderRepository, never()).save(any(Order.class));

            // The customer was already told the order was cancelled. Telling them it is
            // confirmed would be worse than the conflicting command itself.
            verify(eventPublisher, never()).publishOrderConfirmed(any(OrderConfirmedEvent.class));
        }

        @Test
        @DisplayName("CancelOrder on a CONFIRMED order leaves it CONFIRMED and announces nothing")
        void cancelOnConfirmedIsIgnored() {
            Order order = orderIn(OrderStatus.CONFIRMED);
            when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));

            CommandOutcome outcome = orderService.cancelOrder(cancel(order.getId()));

            assertThat(outcome).isEqualTo(CommandOutcome.CONFLICT_IGNORED);
            assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
            verify(orderRepository, never()).save(any(Order.class));
            verify(eventPublisher, never()).publishOrderCancelled(any(OrderCancelledEvent.class));
        }
    }

    @Test
    @DisplayName("a command for an unknown order is ignored rather than thrown, so the consumer cannot spin")
    void unknownOrderIsIgnored() {
        UUID unknownId = UUID.randomUUID();
        when(orderRepository.findById(unknownId)).thenReturn(Optional.empty());

        assertThat(orderService.confirmOrder(confirm(unknownId)))
                .isEqualTo(CommandOutcome.ORDER_NOT_FOUND);

        // Nothing to announce, and nothing to announce it *about* — the event's userId,
        // item and amount all come from an order that does not exist.
        verify(eventPublisher, never()).publishOrderConfirmed(any(OrderConfirmedEvent.class));
    }

    @Test
    @DisplayName("getOrder throws when the order does not exist")
    void getOrderThrowsWhenMissing() {
        UUID unknownId = UUID.randomUUID();
        when(orderRepository.findById(unknownId)).thenReturn(Optional.empty());

        org.assertj.core.api.Assertions
                .assertThatThrownBy(() -> orderService.getOrder(unknownId))
                .isInstanceOf(OrderNotFoundException.class);
    }
}
