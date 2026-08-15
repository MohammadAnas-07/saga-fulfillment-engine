package com.mohammadanas.saga.orchestrator.service;

import com.mohammadanas.saga.orchestrator.config.SagaProperties;
import com.mohammadanas.saga.orchestrator.domain.Saga;
import com.mohammadanas.saga.orchestrator.domain.SagaRepository;
import com.mohammadanas.saga.orchestrator.domain.SagaStatus;
import com.mohammadanas.saga.orchestrator.domain.SagaStep;
import com.mohammadanas.saga.orchestrator.domain.SagaStepRepository;
import com.mohammadanas.saga.orchestrator.messaging.InboundEvents;
import com.mohammadanas.saga.orchestrator.messaging.OutboundMessages;
import com.mohammadanas.saga.orchestrator.messaging.SagaCommandPublisher;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The saga state machine — the only component that decides what happens next
 * (ARCHITECTURE.md section 5.1).
 *
 * <p>Every handler follows the same shape: load the saga, check the transition is legal
 * from the current status, mutate, record a step, publish. Illegal transitions are
 * rejected rather than applied, which is what stops a late or duplicated reply from
 * resurrecting a finished saga.
 */
@Service
public class SagaOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SagaRepository sagaRepository;
    private final SagaStepRepository sagaStepRepository;
    private final SagaCommandPublisher publisher;
    private final SagaProperties properties;
    private final Clock clock;

    public SagaOrchestrator(
            SagaRepository sagaRepository,
            SagaStepRepository sagaStepRepository,
            SagaCommandPublisher publisher,
            SagaProperties properties,
            Clock clock) {
        this.sagaRepository = sagaRepository;
        this.sagaStepRepository = sagaStepRepository;
        this.publisher = publisher;
        this.properties = properties;
        this.clock = clock;
    }

    /** Starts a saga and issues the first command. */
    @Transactional
    public SagaOutcome onOrderCreated(InboundEvents.OrderCreated event) {
        Optional<Saga> existing = sagaRepository.findByOrderId(event.orderId());
        if (existing.isPresent()) {
            log.info("Ignoring OrderCreated for order {}: saga {} already exists",
                    event.orderId(), existing.get().getId());
            return SagaOutcome.IGNORED_DUPLICATE;
        }

        Instant deadline = Instant.now(clock).plus(properties.getTimeout());
        Saga saga = sagaRepository.save(Saga.start(
                event.orderId(), event.userId(), event.itemSku(), event.item(),
                event.quantity(), event.amount(), deadline));

        record(saga, SagaStep.inbound(saga.getId(), "OrderCreated", "order " + event.orderId()));

        publisher.reserveInventory(new OutboundMessages.ReserveInventory(
                UUID.randomUUID(), saga.getId(), saga.getOrderId(), saga.getItemSku(), saga.getQuantity()));
        record(saga, SagaStep.outbound(saga.getId(), "ReserveInventory",
                saga.getItemSku() + " x" + saga.getQuantity()));

        transition(saga, SagaStatus.AWAITING_INVENTORY);
        log.info("Saga {} started for order {}, deadline {}", saga.getId(), saga.getOrderId(), deadline);
        return SagaOutcome.ADVANCED;
    }

    /** Inventory is held; charge the customer. */
    @Transactional
    public SagaOutcome onInventoryReserved(InboundEvents.InventoryReserved event) {
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        if (saga.getStatus() != SagaStatus.AWAITING_INVENTORY) {
            return rejectLateReply(saga, "InventoryReserved");
        }

        record(saga, SagaStep.inbound(saga.getId(), "InventoryReserved", event.item() + " x" + event.quantity()));

        publisher.processPayment(new OutboundMessages.ProcessPayment(
                UUID.randomUUID(), saga.getId(), saga.getOrderId(), saga.getUserId(), saga.getAmount()));
        record(saga, SagaStep.outbound(saga.getId(), "ProcessPayment", "amount " + saga.getAmount()));

        transition(saga, SagaStatus.AWAITING_PAYMENT);
        return SagaOutcome.ADVANCED;
    }

    /**
     * Nothing was reserved and nothing was charged, so there is nothing to undo — this is
     * a straight cancellation, not a compensation (§3.2). The saga goes directly to
     * CANCELLED without passing through COMPENSATING.
     */
    @Transactional
    public SagaOutcome onInventoryReservationFailed(InboundEvents.InventoryReservationFailed event) {
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        if (saga.getStatus() != SagaStatus.AWAITING_INVENTORY) {
            return rejectLateReply(saga, "InventoryReservationFailed");
        }

        record(saga, SagaStep.inbound(saga.getId(), "InventoryReservationFailed", event.reason()));

        cancelOrder(saga, "reservation failed: " + event.reason());
        transition(saga, SagaStatus.CANCELLED);
        return SagaOutcome.ADVANCED;
    }

    /** Payment succeeded; confirm the order and tell the customer. */
    @Transactional
    public SagaOutcome onPaymentCompleted(InboundEvents.PaymentCompleted event) {
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        if (saga.getStatus() != SagaStatus.AWAITING_PAYMENT) {
            return rejectLateReply(saga, "PaymentCompleted");
        }

        record(saga, SagaStep.inbound(saga.getId(), "PaymentCompleted", "payment " + event.paymentId()));

        // The command only. Announcing OrderConfirmed is order-service's job, since it owns
        // the aggregate the fact is about (§3.1).
        publisher.confirmOrder(new OutboundMessages.ConfirmOrder(
                UUID.randomUUID(), saga.getId(), saga.getOrderId()));
        record(saga, SagaStep.outbound(saga.getId(), "ConfirmOrder", null));

        transition(saga, SagaStatus.CONFIRMED);
        log.info("Saga {} CONFIRMED for order {}", saga.getId(), saga.getOrderId());
        return SagaOutcome.ADVANCED;
    }

    /**
     * Payment failed after inventory was reserved: the path that actually needs
     * compensation (§3.3). No money moved, so only the stock must come back.
     *
     * <p>The saga enters COMPENSATING and stays there until {@code InventoryReleased}
     * arrives. It is not marked CANCELLED here — see the class-level note on
     * {@link #onInventoryReleased}.
     */
    @Transactional
    public SagaOutcome onPaymentFailed(InboundEvents.PaymentFailed event) {
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        if (saga.getStatus() != SagaStatus.AWAITING_PAYMENT) {
            return rejectLateReply(saga, "PaymentFailed");
        }

        record(saga, SagaStep.inbound(saga.getId(), "PaymentFailed", event.reason()));
        transition(saga, SagaStatus.COMPENSATING);

        publisher.releaseInventory(new OutboundMessages.ReleaseInventory(
                UUID.randomUUID(), saga.getId(), saga.getOrderId()));
        saga.awaitInventoryRelease();
        record(saga, SagaStep.outbound(saga.getId(), "ReleaseInventory", "compensating"));

        cancelOrder(saga, "payment failed: " + event.reason());

        sagaRepository.save(saga);
        return SagaOutcome.ADVANCED;
    }

    /**
     * A compensating action is confirmed. The saga reaches CANCELLED only once
     * <em>every</em> dispatched compensation has been acknowledged.
     *
     * <p>Waiting rather than marking CANCELLED optimistically is what makes COMPENSATING
     * mean something: while the saga sits there, an undo is genuinely outstanding. The
     * wait cannot deadlock because both confirmations are unconditional — they are
     * published even when there was nothing to undo (§5.3.1).
     */
    @Transactional
    public SagaOutcome onInventoryReleased(InboundEvents.InventoryReleased event) {
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            return rejectLateReply(saga, "InventoryReleased");
        }

        record(saga, SagaStep.inbound(saga.getId(), "InventoryReleased", event.outcome()));
        saga.inventoryReleaseConfirmed();
        return finishCompensationIfComplete(saga);
    }

    @Transactional
    public SagaOutcome onPaymentRefunded(InboundEvents.PaymentRefunded event) {
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            return rejectLateReply(saga, "PaymentRefunded");
        }

        record(saga, SagaStep.inbound(saga.getId(), "PaymentRefunded", event.outcome()));
        saga.paymentRefundConfirmed();
        return finishCompensationIfComplete(saga);
    }

    /**
     * Sagas past their deadline and still non-terminal (§4).
     *
     * <p>Read-only: this reports what is stuck, it does not act. Deciding to compensate is
     * scheduler-service's job, and doing so behind a Redis lock is what keeps two
     * scheduler instances from both acting on the same saga.
     */
    @Transactional(readOnly = true)
    public List<Saga> findStuckSagas() {
        return sagaRepository.findStuckSagas(
                Instant.now(clock), Limit.of(properties.getStuckSagaBatchSize()));
    }

    @Transactional(readOnly = true)
    public List<SagaStep> historyOf(UUID sagaId) {
        return sagaStepRepository.findBySagaIdOrderByIdAsc(sagaId);
    }

    private SagaOutcome finishCompensationIfComplete(Saga saga) {
        if (!saga.compensationComplete()) {
            log.info("Saga {} still compensating: inventoryRelease={} paymentRefund={}",
                    saga.getId(), saga.isAwaitingInventoryRelease(), saga.isAwaitingPaymentRefund());
            sagaRepository.save(saga);
            return SagaOutcome.AWAITING_MORE_COMPENSATION;
        }

        transition(saga, SagaStatus.CANCELLED);
        log.info("Saga {} CANCELLED for order {}: compensation complete", saga.getId(), saga.getOrderId());
        return SagaOutcome.ADVANCED;
    }

    /**
     * Cancels the order. Shared by both cancellation paths.
     *
     * <p>Issues the command only. The customer-facing {@code OrderCancelled} event is
     * order-service's to publish once it applies the status — see §3.1. Until it does,
     * notification-service sees nothing on either terminal path.
     */
    private void cancelOrder(Saga saga, String reason) {
        publisher.cancelOrder(new OutboundMessages.CancelOrder(
                UUID.randomUUID(), saga.getId(), saga.getOrderId()));
        record(saga, SagaStep.outbound(saga.getId(), "CancelOrder", reason));
    }

    private Saga load(UUID sagaId) {
        Saga saga = sagaRepository.findById(sagaId).orElse(null);
        if (saga == null) {
            log.error("Reply for unknown saga {} — ignoring rather than throwing, so the consumer cannot spin",
                    sagaId);
        }
        return saga;
    }

    /**
     * A reply that does not fit the current status is dropped, not applied.
     *
     * <p>This is the guard that stops a late {@code PaymentCompleted} from resurrecting a
     * saga the scheduler already compensated, and stops a redelivered reply from advancing
     * the machine twice.
     */
    private SagaOutcome rejectLateReply(Saga saga, String eventName) {
        log.warn("Ignoring {} for saga {}: not valid from status {}", eventName, saga.getId(), saga.getStatus());
        record(saga, SagaStep.inbound(saga.getId(), eventName + " (ignored)", "invalid from " + saga.getStatus()));
        return SagaOutcome.IGNORED_INVALID_TRANSITION;
    }

    private void transition(Saga saga, SagaStatus target) {
        SagaStatus from = saga.getStatus();
        saga.transitionTo(target);
        sagaRepository.save(saga);
        sagaStepRepository.save(SagaStep.transition(saga.getId(), from, target));
    }

    private void record(Saga saga, SagaStep step) {
        sagaStepRepository.save(step);
    }
}
