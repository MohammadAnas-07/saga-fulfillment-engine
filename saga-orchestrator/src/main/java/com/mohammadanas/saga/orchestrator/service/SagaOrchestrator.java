package com.mohammadanas.saga.orchestrator.service;

import com.mohammadanas.saga.orchestrator.config.SagaProperties;
import com.mohammadanas.saga.orchestrator.domain.ProcessedMessage;
import com.mohammadanas.saga.orchestrator.domain.ProcessedMessageRepository;
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
 * <p>Every handler follows the same shape: reject a message already handled, load the
 * saga, check the transition is legal from the current status, mutate, record a step,
 * publish. Illegal transitions are rejected rather than applied, which is what stops a
 * late reply from resurrecting a finished saga; the dedup check in front of it is what
 * stops a redelivered one from advancing the machine twice (§6).
 */
@Service
public class SagaOrchestrator {

    private static final String ORDER_CREATED = "OrderCreated";
    private static final String INVENTORY_RESERVED = "InventoryReserved";
    private static final String INVENTORY_RESERVATION_FAILED = "InventoryReservationFailed";
    private static final String INVENTORY_RELEASED = "InventoryReleased";
    private static final String PAYMENT_COMPLETED = "PaymentCompleted";
    private static final String PAYMENT_FAILED = "PaymentFailed";
    private static final String PAYMENT_REFUNDED = "PaymentRefunded";

    /** Not a message — the scheduler's REST trigger. Recorded so the history shows why compensation began. */
    private static final String TIMEOUT_SWEEP = "TimeoutSweep";

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private final SagaRepository sagaRepository;
    private final SagaStepRepository sagaStepRepository;
    private final ProcessedMessageRepository processedMessageRepository;
    private final SagaCommandPublisher publisher;
    private final SagaProperties properties;
    private final Clock clock;

    public SagaOrchestrator(
            SagaRepository sagaRepository,
            SagaStepRepository sagaStepRepository,
            ProcessedMessageRepository processedMessageRepository,
            SagaCommandPublisher publisher,
            SagaProperties properties,
            Clock clock) {
        this.sagaRepository = sagaRepository;
        this.sagaStepRepository = sagaStepRepository;
        this.processedMessageRepository = processedMessageRepository;
        this.publisher = publisher;
        this.properties = properties;
        this.clock = clock;
    }

    /** Starts a saga and issues the first command. */
    @Transactional
    public SagaOutcome onOrderCreated(InboundEvents.OrderCreated event) {
        if (alreadyProcessed(event.messageId(), ORDER_CREATED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }

        // Kept alongside the dedup check rather than replaced by it. They catch different
        // things: this catches a *second* OrderCreated for the same order, which is a
        // different message with a different id, not a redelivery of this one.
        Optional<Saga> existing = sagaRepository.findByOrderId(event.orderId());
        if (existing.isPresent()) {
            log.info("Ignoring OrderCreated for order {}: saga {} already exists",
                    event.orderId(), existing.get().getId());
            markProcessed(event.messageId(), ORDER_CREATED);
            return SagaOutcome.IGNORED_DUPLICATE;
        }

        markProcessed(event.messageId(), ORDER_CREATED);

        Instant deadline = Instant.now(clock).plus(properties.getTimeout());
        Saga saga = sagaRepository.save(Saga.start(
                event.orderId(), event.userId(), event.itemSku(), event.item(),
                event.quantity(), event.amount(), deadline));

        record(saga, SagaStep.inbound(saga.getId(), ORDER_CREATED, "order " + event.orderId()));

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
        if (alreadyProcessed(event.messageId(), INVENTORY_RESERVED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        markProcessed(event.messageId(), INVENTORY_RESERVED);

        if (saga.getStatus() != SagaStatus.AWAITING_INVENTORY) {
            return rejectLateReply(saga, INVENTORY_RESERVED);
        }

        record(saga, SagaStep.inbound(saga.getId(), INVENTORY_RESERVED, event.item() + " x" + event.quantity()));

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
        if (alreadyProcessed(event.messageId(), INVENTORY_RESERVATION_FAILED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        markProcessed(event.messageId(), INVENTORY_RESERVATION_FAILED);

        if (saga.getStatus() != SagaStatus.AWAITING_INVENTORY) {
            return rejectLateReply(saga, INVENTORY_RESERVATION_FAILED);
        }

        record(saga, SagaStep.inbound(saga.getId(), INVENTORY_RESERVATION_FAILED, event.reason()));

        cancelOrder(saga, "reservation failed: " + event.reason());
        transition(saga, SagaStatus.CANCELLED);
        return SagaOutcome.ADVANCED;
    }

    /** Payment succeeded; confirm the order and tell the customer. */
    @Transactional
    public SagaOutcome onPaymentCompleted(InboundEvents.PaymentCompleted event) {
        if (alreadyProcessed(event.messageId(), PAYMENT_COMPLETED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        markProcessed(event.messageId(), PAYMENT_COMPLETED);

        if (saga.getStatus() != SagaStatus.AWAITING_PAYMENT) {
            return rejectLateReply(saga, PAYMENT_COMPLETED);
        }

        record(saga, SagaStep.inbound(saga.getId(), PAYMENT_COMPLETED, "payment " + event.paymentId()));

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
        if (alreadyProcessed(event.messageId(), PAYMENT_FAILED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        markProcessed(event.messageId(), PAYMENT_FAILED);

        if (saga.getStatus() != SagaStatus.AWAITING_PAYMENT) {
            return rejectLateReply(saga, PAYMENT_FAILED);
        }

        record(saga, SagaStep.inbound(saga.getId(), PAYMENT_FAILED, event.reason()));
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
        if (alreadyProcessed(event.messageId(), INVENTORY_RELEASED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        markProcessed(event.messageId(), INVENTORY_RELEASED);

        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            return rejectLateReply(saga, INVENTORY_RELEASED);
        }

        record(saga, SagaStep.inbound(saga.getId(), INVENTORY_RELEASED, event.outcome()));
        saga.inventoryReleaseConfirmed();
        return finishCompensationIfComplete(saga);
    }

    @Transactional
    public SagaOutcome onPaymentRefunded(InboundEvents.PaymentRefunded event) {
        if (alreadyProcessed(event.messageId(), PAYMENT_REFUNDED)) {
            return SagaOutcome.IGNORED_REDELIVERY;
        }
        Saga saga = load(event.sagaId());
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }
        markProcessed(event.messageId(), PAYMENT_REFUNDED);

        if (saga.getStatus() != SagaStatus.COMPENSATING) {
            return rejectLateReply(saga, PAYMENT_REFUNDED);
        }

        record(saga, SagaStep.inbound(saga.getId(), PAYMENT_REFUNDED, event.outcome()));
        saga.paymentRefundConfirmed();
        return finishCompensationIfComplete(saga);
    }

    /**
     * Drives a timed-out saga into compensation. The entry point scheduler-service calls
     * once it holds the saga's lock (§4).
     *
     * <p>This routes the timeout into the <em>same</em> compensation the explicit failure
     * path uses — same {@code COMPENSATING} state, same commands, same
     * outstanding-confirmation flags — rather than inventing a second recovery mechanism.
     * §4 is explicit that there should be one compensation code path to reason about, and
     * the scheduler therefore contributes no compensation logic of its own: it decides
     * <em>when</em>, the orchestrator decides <em>what</em>.
     *
     * <p><strong>Which undos are issued depends on where the saga stalled</strong> (§3.4):
     *
     * <ul>
     *   <li>{@code ReleaseInventory} always. The reservation may or may not have happened,
     *       and the confirmation is unconditional either way (§5.3.1), so issuing it
     *       blindly cannot deadlock the wait.
     *   <li>{@code RefundPayment} only from {@code AWAITING_PAYMENT}. That is the §3.5
     *       case: the charge may have succeeded with its reply lost, so this is the branch
     *       that actually returns the customer's money. From
     *       {@code AWAITING_INVENTORY} no payment was ever requested, and asking for a
     *       refund would be asking payment-service about an order it has never seen.
     * </ul>
     *
     * <p>Not guarded by {@code processed_messages}: this is a REST call, not a broker
     * message, so there is no message id to dedup on. The guard is the status check below
     * — a saga already {@code COMPENSATING} or terminal is left alone — backed by the
     * scheduler's Redis lock, which is what stops two instances arriving here at once.
     */
    @Transactional
    public SagaOutcome compensateTimedOut(UUID sagaId) {
        Saga saga = load(sagaId);
        if (saga == null) {
            return SagaOutcome.UNKNOWN_SAGA;
        }

        if (saga.getStatus().isTerminal()) {
            // Common and benign: the saga resolved between the sweep's query and this call.
            log.info("Ignoring timeout compensation for saga {}: already {}", sagaId, saga.getStatus());
            return SagaOutcome.IGNORED_INVALID_TRANSITION;
        }

        if (saga.getStatus() == SagaStatus.COMPENSATING) {
            log.info("Saga {} is already compensating; leaving it to finish", sagaId);
            return SagaOutcome.ALREADY_COMPENSATING;
        }

        SagaStatus stalledIn = saga.getStatus();
        record(saga, SagaStep.inbound(saga.getId(), TIMEOUT_SWEEP, "stalled in " + stalledIn));
        transition(saga, SagaStatus.COMPENSATING);

        publisher.releaseInventory(new OutboundMessages.ReleaseInventory(
                UUID.randomUUID(), saga.getId(), saga.getOrderId()));
        saga.awaitInventoryRelease();
        record(saga, SagaStep.outbound(saga.getId(), "ReleaseInventory", "timeout from " + stalledIn));

        if (stalledIn == SagaStatus.AWAITING_PAYMENT) {
            publisher.refundPayment(new OutboundMessages.RefundPayment(
                    UUID.randomUUID(), saga.getId(), saga.getOrderId()));
            saga.awaitPaymentRefund();
            record(saga, SagaStep.outbound(saga.getId(), "RefundPayment",
                    "timeout from AWAITING_PAYMENT: the charge may have succeeded with its reply lost"));
        }

        cancelOrder(saga, "timed out while " + stalledIn);

        sagaRepository.save(saga);
        log.warn("Saga {} timed out while {} and is now COMPENSATING (order {})",
                sagaId, stalledIn, saga.getOrderId());
        return SagaOutcome.ADVANCED;
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

    /**
     * Fast path for a plain redelivery (§6).
     *
     * <p>As in inventory-service and payment-service, this lookup is only the fast path.
     * The primary key on {@code processed_messages} remains the actual guarantee: two
     * genuinely concurrent deliveries can both pass this check, and the second insert then
     * fails, rolling that transaction back so the machine advances once.
     */
    private boolean alreadyProcessed(UUID messageId, String eventType) {
        if (processedMessageRepository.existsById(messageId)) {
            log.info("Ignoring redelivered {} (messageId={}): already processed", eventType, messageId);
            return true;
        }
        return false;
    }

    /**
     * Marks the message handled, in the same transaction as whatever the handler goes on to
     * do.
     *
     * <p>Called <em>after</em> the saga is known to exist, which is deliberate: an
     * {@code UNKNOWN_SAGA} reply is not marked, so it stays eligible for redelivery. The
     * alternative would permanently swallow a reply whose saga was merely not visible yet,
     * and there is nothing to gain by it — an unknown saga changes no state, so replaying
     * it costs a log line.
     *
     * <p>A reply that <em>is</em> marked and then rejected by the transition guard is still
     * marked, and should be: it was handled, the answer was "no". Leaving it unmarked would
     * append a fresh {@code (ignored)} step to the saga history on every redelivery, which
     * would make an append-only audit trail noisier the flakier the broker got.
     */
    private void markProcessed(UUID messageId, String eventType) {
        processedMessageRepository.save(new ProcessedMessage(messageId, eventType));
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
