package com.mohammadanas.saga.inventory.service;

import com.mohammadanas.saga.inventory.domain.Inventory;
import com.mohammadanas.saga.inventory.domain.InventoryRepository;
import com.mohammadanas.saga.inventory.domain.ProcessedCommand;
import com.mohammadanas.saga.inventory.domain.ProcessedCommandRepository;
import com.mohammadanas.saga.inventory.domain.Reservation;
import com.mohammadanas.saga.inventory.domain.ReservationRepository;
import com.mohammadanas.saga.inventory.domain.ReservationStatus;
import com.mohammadanas.saga.inventory.messaging.InventoryEventPublisher;
import com.mohammadanas.saga.inventory.messaging.InventoryReleasedEvent;
import com.mohammadanas.saga.inventory.messaging.InventoryReservationFailedEvent;
import com.mohammadanas.saga.inventory.messaging.InventoryReservedEvent;
import com.mohammadanas.saga.inventory.messaging.ReleaseInventoryCommand;
import com.mohammadanas.saga.inventory.messaging.ReservationFailureReason;
import com.mohammadanas.saga.inventory.messaging.ReserveInventoryCommand;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Reserves and releases stock on the orchestrator's instruction.
 *
 * <p>Holds no saga logic: it executes the command it is given and reports the result. It
 * never decides what happens next (ARCHITECTURE.md section 5.1).
 */
@Service
public class InventoryService {

    private static final String RESERVE_INVENTORY = "ReserveInventory";
    private static final String RELEASE_INVENTORY = "ReleaseInventory";

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final InventoryRepository inventoryRepository;
    private final ReservationRepository reservationRepository;
    private final ProcessedCommandRepository processedCommandRepository;
    private final InventoryEventPublisher eventPublisher;

    public InventoryService(
            InventoryRepository inventoryRepository,
            ReservationRepository reservationRepository,
            ProcessedCommandRepository processedCommandRepository,
            InventoryEventPublisher eventPublisher) {
        this.inventoryRepository = inventoryRepository;
        this.reservationRepository = reservationRepository;
        this.processedCommandRepository = processedCommandRepository;
        this.eventPublisher = eventPublisher;
    }

    /**
     * Attempts a reservation.
     *
     * <p>The dedup marker, the stock decrement and the reservation row all commit in one
     * transaction. That atomicity is the whole guarantee: a marker able to commit without
     * its stock change would permanently suppress a command that never took effect.
     */
    @Transactional
    public CommandOutcome reserve(ReserveInventoryCommand command) {
        if (alreadyProcessed(command.messageId())) {
            log.info("Ignoring redelivered ReserveInventory (messageId={}) for order {}: already processed",
                    command.messageId(), command.orderId());
            return CommandOutcome.DUPLICATE_IGNORED;
        }

        Optional<Inventory> maybeInventory = inventoryRepository.findByItemIdForUpdate(command.item());

        if (maybeInventory.isEmpty()) {
            log.info("Cannot reserve {} of {} for order {}: no such item",
                    command.quantity(), command.item(), command.orderId());
            return failReservation(command, ReservationFailureReason.UNKNOWN_ITEM);
        }

        Inventory inventory = maybeInventory.get();
        if (!inventory.canReserve(command.quantity())) {
            log.info("Cannot reserve {} of {} for order {}: only {} available",
                    command.quantity(), command.item(), command.orderId(), inventory.getAvailableQuantity());
            return failReservation(command, ReservationFailureReason.INSUFFICIENT_STOCK);
        }

        inventory.reserve(command.quantity());
        inventoryRepository.save(inventory);
        reservationRepository.save(
                Reservation.reserved(command.sagaId(), command.orderId(), command.item(), command.quantity()));
        markProcessed(command.messageId(), RESERVE_INVENTORY);

        eventPublisher.publishInventoryReserved(InventoryReservedEvent.from(
                command.sagaId(), command.orderId(), command.item(), command.quantity()));

        log.info("Reserved {} of {} for order {} ({} available, {} reserved)",
                command.quantity(), command.item(), command.orderId(),
                inventory.getAvailableQuantity(), inventory.getReservedQuantity());
        return CommandOutcome.RESERVED;
    }

    /** Reverses this order's reservation. The compensating action. */
    @Transactional
    public CommandOutcome release(ReleaseInventoryCommand command) {
        if (alreadyProcessed(command.messageId())) {
            log.info("Ignoring redelivered ReleaseInventory (messageId={}) for order {}: already processed",
                    command.messageId(), command.orderId());
            return CommandOutcome.DUPLICATE_IGNORED;
        }

        Optional<Reservation> maybeReservation =
                reservationRepository.findByOrderIdAndStatus(command.orderId(), ReservationStatus.RESERVED);

        if (maybeReservation.isEmpty()) {
            // Still a completed compensation, so it must still be acknowledged. Staying
            // silent here would strand the saga in COMPENSATING forever.
            log.warn("ReleaseInventory (messageId={}) for order {}: no active reservation to reverse; "
                            + "confirming compensation anyway",
                    command.messageId(), command.orderId());
            markProcessed(command.messageId(), RELEASE_INVENTORY);

            eventPublisher.publishInventoryReleased(
                    InventoryReleasedEvent.nothingToReverse(command.sagaId(), command.orderId()));

            return CommandOutcome.NO_ACTIVE_RESERVATION;
        }

        Reservation reservation = maybeReservation.get();
        Inventory inventory = inventoryRepository.findByItemIdForUpdate(reservation.getItemId())
                .orElseThrow(() -> new IllegalStateException(
                        "Reservation " + reservation.getId() + " references unknown item " + reservation.getItemId()));

        inventory.release(reservation.getQuantity());
        inventoryRepository.save(inventory);
        reservation.markReleased();
        reservationRepository.save(reservation);
        markProcessed(command.messageId(), RELEASE_INVENTORY);

        eventPublisher.publishInventoryReleased(InventoryReleasedEvent.reversed(
                command.sagaId(), command.orderId(), reservation.getItemId(), reservation.getQuantity()));

        log.info("Released {} of {} for order {} ({} available, {} reserved)",
                reservation.getQuantity(), reservation.getItemId(), command.orderId(),
                inventory.getAvailableQuantity(), inventory.getReservedQuantity());
        return CommandOutcome.RELEASED;
    }

    private CommandOutcome failReservation(ReserveInventoryCommand command, ReservationFailureReason reason) {
        markProcessed(command.messageId(), RESERVE_INVENTORY);
        eventPublisher.publishInventoryReservationFailed(InventoryReservationFailedEvent.from(
                command.sagaId(), command.orderId(), command.item(), command.quantity(), reason));
        return CommandOutcome.RESERVATION_FAILED;
    }

    /**
     * Fast path for the common case of a plain redelivery. The primary key on
     * {@code processed_commands} remains the actual guarantee: two genuinely concurrent
     * deliveries can both pass this check, and the second insert then fails, rolling that
     * transaction back so no stock is double-reserved.
     */
    private boolean alreadyProcessed(UUID messageId) {
        return processedCommandRepository.existsById(messageId);
    }

    private void markProcessed(UUID messageId, String commandType) {
        processedCommandRepository.save(new ProcessedCommand(messageId, commandType));
    }
}
