package com.mohammadanas.saga.payment.service;

import com.mohammadanas.saga.payment.config.PaymentSimulationProperties;
import com.mohammadanas.saga.payment.domain.Payment;
import com.mohammadanas.saga.payment.domain.PaymentRepository;
import com.mohammadanas.saga.payment.domain.PaymentStatus;
import com.mohammadanas.saga.payment.domain.ProcessedCommand;
import com.mohammadanas.saga.payment.domain.ProcessedCommandRepository;
import com.mohammadanas.saga.payment.messaging.PaymentCompletedEvent;
import com.mohammadanas.saga.payment.messaging.PaymentEventPublisher;
import com.mohammadanas.saga.payment.messaging.PaymentFailedEvent;
import com.mohammadanas.saga.payment.messaging.PaymentFailureReason;
import com.mohammadanas.saga.payment.messaging.PaymentRefundedEvent;
import com.mohammadanas.saga.payment.messaging.ProcessPaymentCommand;
import com.mohammadanas.saga.payment.messaging.RefundPaymentCommand;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Charges and refunds on the orchestrator's instruction.
 *
 * <p>Holds no saga logic: it executes the command it is given and reports the result
 * (ARCHITECTURE.md section 5.1).
 */
@Service
public class PaymentService {

    private static final String PROCESS_PAYMENT = "ProcessPayment";
    private static final String REFUND_PAYMENT = "RefundPayment";

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final ProcessedCommandRepository processedCommandRepository;
    private final PaymentEventPublisher eventPublisher;
    private final PaymentSimulationProperties simulation;

    public PaymentService(
            PaymentRepository paymentRepository,
            ProcessedCommandRepository processedCommandRepository,
            PaymentEventPublisher eventPublisher,
            PaymentSimulationProperties simulation) {
        this.paymentRepository = paymentRepository;
        this.processedCommandRepository = processedCommandRepository;
        this.eventPublisher = eventPublisher;
        this.simulation = simulation;
    }

    /**
     * Attempts a payment.
     *
     * <p>The dedup marker and the payment row commit in one transaction. That atomicity
     * is the guarantee that matters most in this service: a marker able to commit without
     * its charge — or a charge without its marker — is the difference between billing the
     * customer once and billing them twice.
     */
    @Transactional
    public CommandOutcome processPayment(ProcessPaymentCommand command) {
        if (alreadyProcessed(command.messageId())) {
            log.info("Ignoring redelivered ProcessPayment (messageId={}) for order {}: already processed",
                    command.messageId(), command.orderId());
            return CommandOutcome.DUPLICATE_IGNORED;
        }

        if (declines(command)) {
            Payment payment = paymentRepository.save(
                    Payment.failed(command.sagaId(), command.orderId(), command.userId(), command.amount()));
            markProcessed(command.messageId(), PROCESS_PAYMENT);

            eventPublisher.publishPaymentFailed(PaymentFailedEvent.from(
                    command.sagaId(), command.orderId(), payment.getId(), command.amount(),
                    PaymentFailureReason.AMOUNT_ABOVE_THRESHOLD));

            log.info("Payment declined for order {}: amount {} exceeds threshold {}",
                    command.orderId(), command.amount(), simulation.getFailureThreshold());
            return CommandOutcome.PAYMENT_FAILED;
        }

        Payment payment = paymentRepository.save(
                Payment.succeeded(command.sagaId(), command.orderId(), command.userId(), command.amount()));
        markProcessed(command.messageId(), PROCESS_PAYMENT);

        eventPublisher.publishPaymentCompleted(PaymentCompletedEvent.from(
                command.sagaId(), command.orderId(), payment.getId(), command.amount()));

        log.info("Payment {} succeeded for order {}: amount {}",
                payment.getId(), command.orderId(), command.amount());
        return CommandOutcome.PAID;
    }

    /** Reverses this order's successful payment. The compensating action. */
    @Transactional
    public CommandOutcome refundPayment(RefundPaymentCommand command) {
        if (alreadyProcessed(command.messageId())) {
            log.info("Ignoring redelivered RefundPayment (messageId={}) for order {}: already processed",
                    command.messageId(), command.orderId());
            return CommandOutcome.DUPLICATE_IGNORED;
        }

        Optional<Payment> maybePayment =
                paymentRepository.findByOrderIdAndStatus(command.orderId(), PaymentStatus.SUCCEEDED);

        if (maybePayment.isEmpty()) {
            // Legitimate and common: the saga may have failed before payment ran, or the
            // payment itself failed. There is nothing to give back — but compensation is
            // still complete, and staying silent would strand the saga in COMPENSATING.
            log.warn("RefundPayment (messageId={}) for order {}: no successful payment to reverse; "
                            + "confirming compensation anyway",
                    command.messageId(), command.orderId());
            markProcessed(command.messageId(), REFUND_PAYMENT);

            eventPublisher.publishPaymentRefunded(
                    PaymentRefundedEvent.nothingToReverse(command.sagaId(), command.orderId()));

            return CommandOutcome.NO_REFUNDABLE_PAYMENT;
        }

        Payment payment = maybePayment.get();
        payment.refund();
        paymentRepository.save(payment);
        markProcessed(command.messageId(), REFUND_PAYMENT);

        eventPublisher.publishPaymentRefunded(PaymentRefundedEvent.reversed(
                command.sagaId(), command.orderId(), payment.getId(), payment.getAmount()));

        log.info("Refunded payment {} for order {}: amount {}",
                payment.getId(), command.orderId(), payment.getAmount());
        return CommandOutcome.REFUNDED;
    }

    /**
     * The simulated gateway. Deterministic by design — see
     * {@link PaymentSimulationProperties} for how to trigger a failure deliberately.
     */
    private boolean declines(ProcessPaymentCommand command) {
        return command.amount().compareTo(simulation.getFailureThreshold()) > 0;
    }

    /**
     * Fast path for a plain redelivery. The primary key on {@code processed_commands}
     * remains the actual guarantee: two genuinely concurrent deliveries can both pass this
     * check, and the second insert then fails, rolling that transaction back so the
     * customer is not charged twice.
     */
    private boolean alreadyProcessed(UUID messageId) {
        return processedCommandRepository.existsById(messageId);
    }

    private void markProcessed(UUID messageId, String commandType) {
        processedCommandRepository.save(new ProcessedCommand(messageId, commandType));
    }
}
