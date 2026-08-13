package com.mohammadanas.saga.inventory.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.inventory.domain.Inventory;
import com.mohammadanas.saga.inventory.domain.InventoryRepository;
import com.mohammadanas.saga.inventory.domain.ProcessedCommand;
import com.mohammadanas.saga.inventory.domain.ProcessedCommandRepository;
import com.mohammadanas.saga.inventory.domain.Reservation;
import com.mohammadanas.saga.inventory.domain.ReservationRepository;
import com.mohammadanas.saga.inventory.domain.ReservationStatus;
import com.mohammadanas.saga.inventory.messaging.CompensationOutcome;
import com.mohammadanas.saga.inventory.messaging.InventoryEventPublisher;
import com.mohammadanas.saga.inventory.messaging.InventoryReleasedEvent;
import com.mohammadanas.saga.inventory.messaging.InventoryReservationFailedEvent;
import com.mohammadanas.saga.inventory.messaging.InventoryReservedEvent;
import com.mohammadanas.saga.inventory.messaging.ReleaseInventoryCommand;
import com.mohammadanas.saga.inventory.messaging.ReservationFailureReason;
import com.mohammadanas.saga.inventory.messaging.ReserveInventoryCommand;
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
 * Unit tests for reservation and compensation, with no infrastructure.
 *
 * <p>The redelivery cases carry the weight here. Kafka is at-least-once, so a replayed
 * ReserveInventory is normal operation, and without the processed_commands guard it
 * silently reserves the same stock twice (ARCHITECTURE.md section 6).
 */
@ExtendWith(MockitoExtension.class)
class InventoryServiceTest {

    private static final String ITEM = "MECH-KB-01";

    @Mock
    private InventoryRepository inventoryRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private ProcessedCommandRepository processedCommandRepository;

    @Mock
    private InventoryEventPublisher eventPublisher;

    @InjectMocks
    private InventoryService inventoryService;

    private static ReserveInventoryCommand reserveCommand(int quantity) {
        return new ReserveInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), ITEM, quantity);
    }

    private static ReleaseInventoryCommand releaseCommand(UUID orderId) {
        return new ReleaseInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), orderId);
    }

    @Nested
    @DisplayName("successful reservation")
    class SuccessfulReservation {

        @Test
        @DisplayName("decrements available, increments reserved, and publishes InventoryReserved")
        void reservesStock() {
            Inventory inventory = new Inventory(ITEM, 10);
            ReserveInventoryCommand command = reserveCommand(3);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            CommandOutcome outcome = inventoryService.reserve(command);

            assertThat(outcome).isEqualTo(CommandOutcome.RESERVED);
            assertThat(inventory.getAvailableQuantity()).isEqualTo(7);
            assertThat(inventory.getReservedQuantity()).isEqualTo(3);

            ArgumentCaptor<InventoryReservedEvent> captor =
                    ArgumentCaptor.forClass(InventoryReservedEvent.class);
            verify(eventPublisher).publishInventoryReserved(captor.capture());
            assertThat(captor.getValue().orderId()).isEqualTo(command.orderId());
            assertThat(captor.getValue().item()).isEqualTo(ITEM);
            assertThat(captor.getValue().quantity()).isEqualTo(3);
        }

        @Test
        @DisplayName("records a reservation row so compensation knows how much to hand back")
        void recordsReservation() {
            Inventory inventory = new Inventory(ITEM, 10);
            ReserveInventoryCommand command = reserveCommand(4);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            inventoryService.reserve(command);

            ArgumentCaptor<Reservation> captor = ArgumentCaptor.forClass(Reservation.class);
            verify(reservationRepository).save(captor.capture());
            assertThat(captor.getValue().getOrderId()).isEqualTo(command.orderId());
            assertThat(captor.getValue().getQuantity()).isEqualTo(4);
            assertThat(captor.getValue().getStatus()).isEqualTo(ReservationStatus.RESERVED);
        }
    }

    @Nested
    @DisplayName("failed reservation")
    class FailedReservation {

        @Test
        @DisplayName("insufficient stock publishes InventoryReservationFailed and leaves stock untouched")
        void insufficientStock() {
            Inventory inventory = new Inventory(ITEM, 2);
            ReserveInventoryCommand command = reserveCommand(5);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            CommandOutcome outcome = inventoryService.reserve(command);

            assertThat(outcome).isEqualTo(CommandOutcome.RESERVATION_FAILED);
            assertThat(inventory.getAvailableQuantity()).isEqualTo(2);
            assertThat(inventory.getReservedQuantity()).isZero();
            verify(reservationRepository, never()).save(any(Reservation.class));

            ArgumentCaptor<InventoryReservationFailedEvent> captor =
                    ArgumentCaptor.forClass(InventoryReservationFailedEvent.class);
            verify(eventPublisher).publishInventoryReservationFailed(captor.capture());
            assertThat(captor.getValue().reason()).isEqualTo(ReservationFailureReason.INSUFFICIENT_STOCK);
        }

        @Test
        @DisplayName("an unknown item fails with UNKNOWN_ITEM rather than throwing")
        void unknownItem() {
            ReserveInventoryCommand command = reserveCommand(1);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.empty());

            CommandOutcome outcome = inventoryService.reserve(command);

            assertThat(outcome).isEqualTo(CommandOutcome.RESERVATION_FAILED);
            ArgumentCaptor<InventoryReservationFailedEvent> captor =
                    ArgumentCaptor.forClass(InventoryReservationFailedEvent.class);
            verify(eventPublisher).publishInventoryReservationFailed(captor.capture());
            assertThat(captor.getValue().reason()).isEqualTo(ReservationFailureReason.UNKNOWN_ITEM);
        }

        @Test
        @DisplayName("a boundary request for exactly the available quantity still succeeds")
        void exactlyEnoughStock() {
            Inventory inventory = new Inventory(ITEM, 5);
            ReserveInventoryCommand command = reserveCommand(5);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            assertThat(inventoryService.reserve(command)).isEqualTo(CommandOutcome.RESERVED);
            assertThat(inventory.getAvailableQuantity()).isZero();
            assertThat(inventory.getReservedQuantity()).isEqualTo(5);
        }
    }

    @Nested
    @DisplayName("release reverses a reservation")
    class Release {

        @Test
        @DisplayName("restores the exact reserved quantity and publishes InventoryReleased")
        void releasesStock() {
            Inventory inventory = new Inventory(ITEM, 10);
            inventory.reserve(3);
            UUID orderId = UUID.randomUUID();
            Reservation reservation = Reservation.reserved(UUID.randomUUID(), orderId, ITEM, 3);
            ReleaseInventoryCommand command = releaseCommand(orderId);

            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED))
                    .thenReturn(Optional.of(reservation));
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            CommandOutcome outcome = inventoryService.release(command);

            assertThat(outcome).isEqualTo(CommandOutcome.RELEASED);
            assertThat(inventory.getAvailableQuantity()).isEqualTo(10);
            assertThat(inventory.getReservedQuantity()).isZero();
            assertThat(reservation.getStatus()).isEqualTo(ReservationStatus.RELEASED);

            ArgumentCaptor<InventoryReleasedEvent> captor = ArgumentCaptor.forClass(InventoryReleasedEvent.class);
            verify(eventPublisher).publishInventoryReleased(captor.capture());
            assertThat(captor.getValue().quantity()).isEqualTo(3);
            assertThat(captor.getValue().item()).isEqualTo(ITEM);
            assertThat(captor.getValue().outcome()).isEqualTo(CompensationOutcome.REVERSED);
        }

        @Test
        @DisplayName("release with no active reservation is a no-op, not an error")
        void noActiveReservation() {
            UUID orderId = UUID.randomUUID();
            ReleaseInventoryCommand command = releaseCommand(orderId);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED))
                    .thenReturn(Optional.empty());

            CommandOutcome outcome = inventoryService.release(command);

            assertThat(outcome).isEqualTo(CommandOutcome.NO_ACTIVE_RESERVATION);
            verify(inventoryRepository, never()).save(any(Inventory.class));
        }
    }

    @Nested
    @DisplayName("compensation is acknowledged even when there was nothing to undo")
    class CompensationAlwaysAcknowledged {

        @Test
        @DisplayName("release with no active reservation STILL publishes InventoryReleased")
        void noActiveReservationStillPublishes() {
            UUID orderId = UUID.randomUUID();
            ReleaseInventoryCommand command = releaseCommand(orderId);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED))
                    .thenReturn(Optional.empty());

            inventoryService.release(command);

            // The behaviour this change exists for. Silence here would strand the saga in
            // COMPENSATING forever, waiting on an acknowledgement that never comes.
            ArgumentCaptor<InventoryReleasedEvent> captor =
                    ArgumentCaptor.forClass(InventoryReleasedEvent.class);
            verify(eventPublisher).publishInventoryReleased(captor.capture());
            assertThat(captor.getValue().orderId()).isEqualTo(orderId);
            assertThat(captor.getValue().sagaId()).isEqualTo(command.sagaId());
        }

        @Test
        @DisplayName("that event is marked NOTHING_TO_REVERSE so the audit trail stays honest")
        void noOpEventIsDistinguishable() {
            UUID orderId = UUID.randomUUID();
            ReleaseInventoryCommand command = releaseCommand(orderId);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED))
                    .thenReturn(Optional.empty());

            inventoryService.release(command);

            ArgumentCaptor<InventoryReleasedEvent> captor =
                    ArgumentCaptor.forClass(InventoryReleasedEvent.class);
            verify(eventPublisher).publishInventoryReleased(captor.capture());

            InventoryReleasedEvent event = captor.getValue();
            assertThat(event.outcome()).isEqualTo(CompensationOutcome.NOTHING_TO_REVERSE);
            assertThat(event.quantity()).isZero();
            assertThat(event.item()).isNull();
        }

        @Test
        @DisplayName("a redelivered ReleaseInventory stays silent — the same outcome, not a new one")
        void duplicateStaysSilent() {
            ReleaseInventoryCommand command = releaseCommand(UUID.randomUUID());
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(true);

            CommandOutcome outcome = inventoryService.release(command);

            // The deliberate exception: idempotency exists to suppress a repeated report
            // of an outcome already acknowledged.
            assertThat(outcome).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            verifyNoInteractions(eventPublisher);
        }
    }

    @Nested
    @DisplayName("idempotency — a redelivered command must not double-apply")
    class Idempotency {

        @Test
        @DisplayName("a redelivered ReserveInventory does not reserve a second time")
        void redeliveredReserveDoesNotDoubleReserve() {
            ReserveInventoryCommand command = reserveCommand(3);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(true);

            CommandOutcome outcome = inventoryService.reserve(command);

            assertThat(outcome).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            // The decisive assertions: stock was never even loaded, nothing was written,
            // and no event was emitted.
            verifyNoInteractions(inventoryRepository);
            verifyNoInteractions(reservationRepository);
            verifyNoInteractions(eventPublisher);
            verify(processedCommandRepository, never()).save(any(ProcessedCommand.class));
        }

        @Test
        @DisplayName("delivering the same ReserveInventory twice reserves the stock exactly once")
        void sameCommandTwiceReservesOnce() {
            Inventory inventory = new Inventory(ITEM, 10);
            ReserveInventoryCommand command = reserveCommand(3);

            // First delivery: not yet seen. Second: the marker written by the first.
            when(processedCommandRepository.existsById(command.messageId()))
                    .thenReturn(false)
                    .thenReturn(true);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            CommandOutcome first = inventoryService.reserve(command);
            CommandOutcome second = inventoryService.reserve(command);

            assertThat(first).isEqualTo(CommandOutcome.RESERVED);
            assertThat(second).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);

            // 3 units taken, not 6 — the whole point of this chunk.
            assertThat(inventory.getAvailableQuantity()).isEqualTo(7);
            assertThat(inventory.getReservedQuantity()).isEqualTo(3);
            verify(eventPublisher).publishInventoryReserved(any(InventoryReservedEvent.class));
        }

        @Test
        @DisplayName("a redelivered ReleaseInventory does not release twice")
        void redeliveredReleaseDoesNotDoubleRelease() {
            ReleaseInventoryCommand command = releaseCommand(UUID.randomUUID());
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(true);

            CommandOutcome outcome = inventoryService.release(command);

            assertThat(outcome).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            verifyNoInteractions(inventoryRepository);
            verifyNoInteractions(eventPublisher);
        }

        @Test
        @DisplayName("the dedup marker is written for every command that is actually handled")
        void marksCommandsProcessed() {
            Inventory inventory = new Inventory(ITEM, 10);
            ReserveInventoryCommand command = reserveCommand(1);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            inventoryService.reserve(command);

            ArgumentCaptor<ProcessedCommand> captor = ArgumentCaptor.forClass(ProcessedCommand.class);
            verify(processedCommandRepository).save(captor.capture());
            assertThat(captor.getValue().getMessageId()).isEqualTo(command.messageId());
            assertThat(captor.getValue().getCommandType()).isEqualTo("ReserveInventory");
        }

        @Test
        @DisplayName("a failed reservation is also marked processed, so replaying it cannot succeed later")
        void failedReservationIsAlsoMarkedProcessed() {
            Inventory inventory = new Inventory(ITEM, 1);
            ReserveInventoryCommand command = reserveCommand(5);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(inventoryRepository.findByItemIdForUpdate(ITEM)).thenReturn(Optional.of(inventory));

            inventoryService.reserve(command);

            verify(processedCommandRepository).save(any(ProcessedCommand.class));
        }
    }
}
