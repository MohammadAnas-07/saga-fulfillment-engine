package com.mohammadanas.saga.orchestrator.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.orchestrator.config.SagaProperties;
import com.mohammadanas.saga.orchestrator.domain.Saga;
import com.mohammadanas.saga.orchestrator.domain.SagaRepository;
import com.mohammadanas.saga.orchestrator.domain.SagaStatus;
import com.mohammadanas.saga.orchestrator.domain.SagaStep;
import com.mohammadanas.saga.orchestrator.domain.SagaStepRepository;
import com.mohammadanas.saga.orchestrator.messaging.InboundEvents;
import com.mohammadanas.saga.orchestrator.messaging.OutboundMessages;
import com.mohammadanas.saga.orchestrator.messaging.SagaCommandPublisher;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Limit;

/**
 * The state machine, exercised on every path.
 *
 * <p>The compensation and rejection cases outnumber the happy path here on purpose. A
 * saga that succeeds proves very little; the value of the pattern is entirely in what
 * happens when a step fails, when a reply arrives late, and when compensation is only
 * half finished (ARCHITECTURE.md section 8.3).
 */
@ExtendWith(MockitoExtension.class)
class SagaOrchestratorTest {

    private static final Instant NOW = Instant.parse("2026-08-15T10:00:00Z");
    private static final Duration TIMEOUT = Duration.ofMinutes(5);

    private static final String USER_ID = "user-42";
    private static final String ITEM_SKU = "MECH-KB-01";
    private static final String ITEM = "Mechanical keyboard";
    private static final BigDecimal AMOUNT = new BigDecimal("99.98");

    @Mock
    private SagaRepository sagaRepository;

    @Mock
    private SagaStepRepository sagaStepRepository;

    @Mock
    private SagaCommandPublisher publisher;

    private SagaProperties properties;
    private SagaOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        properties = new SagaProperties();
        properties.setTimeout(TIMEOUT);
        orchestrator = new SagaOrchestrator(
                sagaRepository, sagaStepRepository, publisher, properties,
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    private static Saga sagaIn(SagaStatus status) {
        Saga saga = Saga.start(UUID.randomUUID(), USER_ID, ITEM_SKU, ITEM, 2, AMOUNT, NOW.plus(TIMEOUT));
        if (status != SagaStatus.STARTED) {
            saga.transitionTo(status);
        }
        return saga;
    }

    private static InboundEvents.OrderCreated orderCreated(UUID orderId) {
        return new InboundEvents.OrderCreated(
                UUID.randomUUID(), orderId, USER_ID, ITEM_SKU, ITEM, 2,
                new BigDecimal("49.99"), AMOUNT, NOW);
    }

    @Nested
    @DisplayName("happy path")
    class HappyPath {

        @Test
        @DisplayName("OrderCreated starts a saga, stamps a deadline, and issues ReserveInventory")
        void startsSaga() {
            UUID orderId = UUID.randomUUID();
            when(sagaRepository.findByOrderId(orderId)).thenReturn(Optional.empty());
            when(sagaRepository.save(any(Saga.class))).thenAnswer(inv -> inv.getArgument(0));

            SagaOutcome outcome = orchestrator.onOrderCreated(orderCreated(orderId));

            assertThat(outcome).isEqualTo(SagaOutcome.ADVANCED);

            ArgumentCaptor<Saga> sagaCaptor = ArgumentCaptor.forClass(Saga.class);
            verify(sagaRepository, org.mockito.Mockito.atLeastOnce()).save(sagaCaptor.capture());
            Saga saved = sagaCaptor.getValue();
            assertThat(saved.getStatus()).isEqualTo(SagaStatus.AWAITING_INVENTORY);
            assertThat(saved.getTimeoutDeadline()).isEqualTo(NOW.plus(TIMEOUT));
            assertThat(saved.getOrderId()).isEqualTo(orderId);

            ArgumentCaptor<OutboundMessages.ReserveInventory> captor =
                    ArgumentCaptor.forClass(OutboundMessages.ReserveInventory.class);
            verify(publisher).reserveInventory(captor.capture());
            // The SKU is forwarded, not the display text — inventory matches on itemId.
            assertThat(captor.getValue().item()).isEqualTo(ITEM_SKU);
            assertThat(captor.getValue().quantity()).isEqualTo(2);
        }

        @Test
        @DisplayName("InventoryReserved issues ProcessPayment for the order total and awaits payment")
        void reservedLeadsToPayment() {
            Saga saga = sagaIn(SagaStatus.AWAITING_INVENTORY);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onInventoryReserved(new InboundEvents.InventoryReserved(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), ITEM_SKU, 2, NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.ADVANCED);
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.AWAITING_PAYMENT);

            ArgumentCaptor<OutboundMessages.ProcessPayment> captor =
                    ArgumentCaptor.forClass(OutboundMessages.ProcessPayment.class);
            verify(publisher).processPayment(captor.capture());
            assertThat(captor.getValue().amount()).isEqualByComparingTo(AMOUNT);
            assertThat(captor.getValue().userId()).isEqualTo(USER_ID);
        }

        @Test
        @DisplayName("PaymentCompleted issues ConfirmOrder and marks the saga CONFIRMED")
        void paymentCompletedConfirms() {
            Saga saga = sagaIn(SagaStatus.AWAITING_PAYMENT);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onPaymentCompleted(new InboundEvents.PaymentCompleted(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(), AMOUNT, NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.ADVANCED);
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CONFIRMED);

            ArgumentCaptor<OutboundMessages.ConfirmOrder> captor =
                    ArgumentCaptor.forClass(OutboundMessages.ConfirmOrder.class);
            verify(publisher).confirmOrder(captor.capture());
            assertThat(captor.getValue().sagaId()).isEqualTo(saga.getId());
            assertThat(captor.getValue().orderId()).isEqualTo(saga.getOrderId());

            // Nothing compensating on the success path.
            verify(publisher, never()).releaseInventory(any());
            verify(publisher, never()).cancelOrder(any());
        }
    }

    @Nested
    @DisplayName("failure path A — inventory reservation fails (no compensation needed)")
    class InventoryFailure {

        @Test
        @DisplayName("cancels the order and goes straight to CANCELLED, skipping COMPENSATING")
        void cancelsWithoutCompensating() {
            Saga saga = sagaIn(SagaStatus.AWAITING_INVENTORY);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onInventoryReservationFailed(
                    new InboundEvents.InventoryReservationFailed(UUID.randomUUID(), saga.getId(),
                            saga.getOrderId(), ITEM_SKU, 2, "INSUFFICIENT_STOCK", NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.ADVANCED);
            // Straight to CANCELLED: nothing was reserved, so there is nothing to undo.
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CANCELLED);
            verify(publisher).cancelOrder(any(OutboundMessages.CancelOrder.class));
            verify(publisher, never()).releaseInventory(any());
        }

        @Test
        @DisplayName("never issues ProcessPayment — the customer must not be charged")
        void neverCharges() {
            Saga saga = sagaIn(SagaStatus.AWAITING_INVENTORY);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            orchestrator.onInventoryReservationFailed(
                    new InboundEvents.InventoryReservationFailed(UUID.randomUUID(), saga.getId(),
                            saga.getOrderId(), ITEM_SKU, 2, "UNKNOWN_ITEM", NOW));

            verify(publisher, never()).processPayment(any());
        }
    }

    @Nested
    @DisplayName("failure path B — payment fails after inventory was reserved")
    class PaymentFailure {

        @Test
        @DisplayName("enters COMPENSATING and releases the stock, rather than cancelling immediately")
        void entersCompensating() {
            Saga saga = sagaIn(SagaStatus.AWAITING_PAYMENT);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onPaymentFailed(new InboundEvents.PaymentFailed(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(),
                    AMOUNT, "AMOUNT_ABOVE_THRESHOLD", NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.ADVANCED);
            // The assertion that matters: it is COMPENSATING, not CANCELLED. A test that
            // only checked the final status would pass even if compensation never ran.
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
            assertThat(saga.isAwaitingInventoryRelease()).isTrue();

            verify(publisher).releaseInventory(any(OutboundMessages.ReleaseInventory.class));
            verify(publisher).cancelOrder(any(OutboundMessages.CancelOrder.class));
        }

        @Test
        @DisplayName("issues no refund — payment failed, so no money moved")
        void noRefundWhenPaymentFailed() {
            Saga saga = sagaIn(SagaStatus.AWAITING_PAYMENT);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            orchestrator.onPaymentFailed(new InboundEvents.PaymentFailed(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(),
                    AMOUNT, "AMOUNT_ABOVE_THRESHOLD", NOW));

            verify(publisher, never()).refundPayment(any());
            assertThat(saga.isAwaitingPaymentRefund()).isFalse();
        }

        @Test
        @DisplayName("reaches CANCELLED only once InventoryReleased confirms the undo")
        void cancelledOnlyAfterConfirmation() {
            Saga saga = sagaIn(SagaStatus.AWAITING_PAYMENT);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            orchestrator.onPaymentFailed(new InboundEvents.PaymentFailed(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(),
                    AMOUNT, "AMOUNT_ABOVE_THRESHOLD", NOW));
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);

            orchestrator.onInventoryReleased(new InboundEvents.InventoryReleased(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), ITEM_SKU, 2, "REVERSED", NOW));

            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CANCELLED);
            assertThat(saga.isAwaitingInventoryRelease()).isFalse();
        }

        @Test
        @DisplayName("a NOTHING_TO_REVERSE confirmation still completes compensation")
        void nothingToReverseStillCompletes() {
            Saga saga = sagaIn(SagaStatus.COMPENSATING);
            saga.awaitInventoryRelease();
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            // Unconditional confirmations (§5.3.1) are what stop the wait deadlocking when
            // there was nothing to undo.
            orchestrator.onInventoryReleased(new InboundEvents.InventoryReleased(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), null, 0, "NOTHING_TO_REVERSE", NOW));

            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CANCELLED);
        }
    }

    @Nested
    @DisplayName("compensation is only complete when every undo is confirmed")
    class PartialCompensation {

        @Test
        @DisplayName("one of two confirmations leaves the saga in COMPENSATING")
        void oneOfTwoIsNotEnough() {
            Saga saga = sagaIn(SagaStatus.COMPENSATING);
            saga.awaitInventoryRelease();
            saga.awaitPaymentRefund();
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onInventoryReleased(new InboundEvents.InventoryReleased(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), ITEM_SKU, 2, "REVERSED", NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.AWAITING_MORE_COMPENSATION);
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);
            assertThat(saga.isAwaitingPaymentRefund()).isTrue();
        }

        @Test
        @DisplayName("the second confirmation completes it, in either order")
        void secondConfirmationCompletes() {
            Saga saga = sagaIn(SagaStatus.COMPENSATING);
            saga.awaitInventoryRelease();
            saga.awaitPaymentRefund();
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            // Refund confirms first this time — order must not matter.
            orchestrator.onPaymentRefunded(new InboundEvents.PaymentRefunded(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(),
                    AMOUNT, "REVERSED", NOW));
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.COMPENSATING);

            orchestrator.onInventoryReleased(new InboundEvents.InventoryReleased(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), ITEM_SKU, 2, "REVERSED", NOW));

            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CANCELLED);
        }
    }

    @Nested
    @DisplayName("late and invalid replies are dropped, never applied")
    class InvalidTransitions {

        @Test
        @DisplayName("PaymentCompleted arriving after compensation does not resurrect the saga")
        void lateSuccessDoesNotResurrect() {
            Saga saga = sagaIn(SagaStatus.CANCELLED);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onPaymentCompleted(new InboundEvents.PaymentCompleted(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(), AMOUNT, NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.IGNORED_INVALID_TRANSITION);
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CANCELLED);
            verify(publisher, never()).confirmOrder(any());
        }

        @Test
        @DisplayName("a duplicated InventoryReserved does not issue ProcessPayment twice")
        void duplicateReservedIgnored() {
            Saga saga = sagaIn(SagaStatus.AWAITING_PAYMENT);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            SagaOutcome outcome = orchestrator.onInventoryReserved(new InboundEvents.InventoryReserved(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), ITEM_SKU, 2, NOW));

            assertThat(outcome).isEqualTo(SagaOutcome.IGNORED_INVALID_TRANSITION);
            verify(publisher, never()).processPayment(any());
        }

        @Test
        @DisplayName("PaymentFailed on an already CONFIRMED saga is dropped")
        void failureAfterConfirmIgnored() {
            Saga saga = sagaIn(SagaStatus.CONFIRMED);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            assertThat(orchestrator.onPaymentFailed(new InboundEvents.PaymentFailed(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(),
                    AMOUNT, "AMOUNT_ABOVE_THRESHOLD", NOW)))
                    .isEqualTo(SagaOutcome.IGNORED_INVALID_TRANSITION);
            assertThat(saga.getStatus()).isEqualTo(SagaStatus.CONFIRMED);
            verify(publisher, never()).releaseInventory(any());
        }

        @Test
        @DisplayName("a second OrderCreated for the same order does not start a second saga")
        void duplicateOrderCreatedIgnored() {
            UUID orderId = UUID.randomUUID();
            when(sagaRepository.findByOrderId(orderId)).thenReturn(Optional.of(sagaIn(SagaStatus.AWAITING_PAYMENT)));

            assertThat(orchestrator.onOrderCreated(orderCreated(orderId)))
                    .isEqualTo(SagaOutcome.IGNORED_DUPLICATE);
            verify(sagaRepository, never()).save(any(Saga.class));
            verifyNoInteractions(publisher);
        }

        @Test
        @DisplayName("a reply for an unknown saga is ignored rather than thrown, so the consumer cannot spin")
        void unknownSagaIgnored() {
            UUID sagaId = UUID.randomUUID();
            when(sagaRepository.findById(sagaId)).thenReturn(Optional.empty());

            assertThat(orchestrator.onInventoryReserved(new InboundEvents.InventoryReserved(
                    UUID.randomUUID(), sagaId, UUID.randomUUID(), ITEM_SKU, 2, NOW)))
                    .isEqualTo(SagaOutcome.UNKNOWN_SAGA);
            verifyNoInteractions(publisher);
        }
    }

    @Nested
    @DisplayName("stuck-saga query")
    class StuckSagas {

        @Test
        @DisplayName("asks for sagas past the current clock, capped at the configured batch size")
        void queriesWithCurrentTimeAndLimit() {
            properties.setStuckSagaBatchSize(25);
            Saga stuck = sagaIn(SagaStatus.AWAITING_PAYMENT);
            when(sagaRepository.findStuckSagas(any(Instant.class), any(Limit.class)))
                    .thenReturn(List.of(stuck));

            List<Saga> result = orchestrator.findStuckSagas();

            assertThat(result).containsExactly(stuck);

            ArgumentCaptor<Instant> nowCaptor = ArgumentCaptor.forClass(Instant.class);
            ArgumentCaptor<Limit> limitCaptor = ArgumentCaptor.forClass(Limit.class);
            verify(sagaRepository).findStuckSagas(nowCaptor.capture(), limitCaptor.capture());

            // Driven by the injected clock, so the timeout path is testable without sleeping.
            assertThat(nowCaptor.getValue()).isEqualTo(NOW);
            assertThat(limitCaptor.getValue().max()).isEqualTo(25);
        }

        @Test
        @DisplayName("returns nothing when no saga is past its deadline")
        void emptyWhenNothingStuck() {
            when(sagaRepository.findStuckSagas(any(Instant.class), any(Limit.class))).thenReturn(List.of());

            assertThat(orchestrator.findStuckSagas()).isEmpty();
        }

        @Test
        @DisplayName("reports without acting — deciding to compensate is the scheduler's job")
        void isReadOnly() {
            when(sagaRepository.findStuckSagas(any(Instant.class), any(Limit.class)))
                    .thenReturn(List.of(sagaIn(SagaStatus.AWAITING_INVENTORY)));

            orchestrator.findStuckSagas();

            verifyNoInteractions(publisher);
            verify(sagaRepository, never()).save(any(Saga.class));
        }
    }

    @Nested
    @DisplayName("saga history")
    class History {

        @Test
        @DisplayName("every handled event and issued command is recorded as a step")
        void recordsSteps() {
            Saga saga = sagaIn(SagaStatus.AWAITING_INVENTORY);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            orchestrator.onInventoryReserved(new InboundEvents.InventoryReserved(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), ITEM_SKU, 2, NOW));

            ArgumentCaptor<SagaStep> captor = ArgumentCaptor.forClass(SagaStep.class);
            verify(sagaStepRepository, org.mockito.Mockito.atLeast(3)).save(captor.capture());

            // Inbound event, outbound command, and the transition all leave a trace.
            assertThat(captor.getAllValues()).extracting(SagaStep::getStep)
                    .contains("InventoryReserved", "ProcessPayment", "AWAITING_INVENTORY -> AWAITING_PAYMENT");
        }

        @Test
        @DisplayName("an ignored reply is recorded too, so a dropped message is explainable later")
        void recordsIgnoredReplies() {
            Saga saga = sagaIn(SagaStatus.CANCELLED);
            when(sagaRepository.findById(saga.getId())).thenReturn(Optional.of(saga));

            orchestrator.onPaymentCompleted(new InboundEvents.PaymentCompleted(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId(), UUID.randomUUID(), AMOUNT, NOW));

            ArgumentCaptor<SagaStep> captor = ArgumentCaptor.forClass(SagaStep.class);
            verify(sagaStepRepository).save(captor.capture());
            assertThat(captor.getValue().getStep()).isEqualTo("PaymentCompleted (ignored)");
        }
    }
}
