package com.mohammadanas.saga.payment.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

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
import com.mohammadanas.saga.payment.messaging.ProcessPaymentCommand;
import com.mohammadanas.saga.payment.messaging.RefundPaymentCommand;
import java.math.BigDecimal;
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

/**
 * Unit tests for payment and its compensation, with no infrastructure.
 *
 * <p>The duplicate-command case is the one that matters most in this service: a
 * redelivered ProcessPayment that is not caught bills a real customer twice
 * (ARCHITECTURE.md section 6).
 */
@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    private static final String USER_ID = "user-1";
    private static final BigDecimal THRESHOLD = new BigDecimal("1000.00");

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private ProcessedCommandRepository processedCommandRepository;

    @Mock
    private PaymentEventPublisher eventPublisher;

    private PaymentService paymentService;

    @BeforeEach
    void setUp() {
        PaymentSimulationProperties simulation = new PaymentSimulationProperties();
        simulation.setFailureThreshold(THRESHOLD);
        paymentService = new PaymentService(
                paymentRepository, processedCommandRepository, eventPublisher, simulation);
    }

    private static ProcessPaymentCommand processCommand(BigDecimal amount) {
        return new ProcessPaymentCommand(
                UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), USER_ID, amount);
    }

    private static RefundPaymentCommand refundCommand(UUID orderId) {
        return new RefundPaymentCommand(UUID.randomUUID(), UUID.randomUUID(), orderId);
    }

    @Nested
    @DisplayName("successful payment")
    class SuccessfulPayment {

        @Test
        @DisplayName("an amount at or below the threshold succeeds and publishes PaymentCompleted")
        void succeedsBelowThreshold() {
            ProcessPaymentCommand command = processCommand(new BigDecimal("99.98"));
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            CommandOutcome outcome = paymentService.processPayment(command);

            assertThat(outcome).isEqualTo(CommandOutcome.PAID);

            ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
            verify(paymentRepository).save(paymentCaptor.capture());
            assertThat(paymentCaptor.getValue().getStatus()).isEqualTo(PaymentStatus.SUCCEEDED);
            assertThat(paymentCaptor.getValue().getAmount()).isEqualByComparingTo("99.98");
            assertThat(paymentCaptor.getValue().getOrderId()).isEqualTo(command.orderId());

            ArgumentCaptor<PaymentCompletedEvent> eventCaptor =
                    ArgumentCaptor.forClass(PaymentCompletedEvent.class);
            verify(eventPublisher).publishPaymentCompleted(eventCaptor.capture());
            assertThat(eventCaptor.getValue().orderId()).isEqualTo(command.orderId());
            assertThat(eventCaptor.getValue().amount()).isEqualByComparingTo("99.98");
        }

        @Test
        @DisplayName("exactly the threshold succeeds — the boundary is inclusive")
        void exactlyThresholdSucceeds() {
            ProcessPaymentCommand command = processCommand(THRESHOLD);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            assertThat(paymentService.processPayment(command)).isEqualTo(CommandOutcome.PAID);
            verify(eventPublisher).publishPaymentCompleted(any(PaymentCompletedEvent.class));
        }
    }

    @Nested
    @DisplayName("failed payment — the documented trigger")
    class FailedPayment {

        @Test
        @DisplayName("an amount above the threshold fails with AMOUNT_ABOVE_THRESHOLD")
        void failsAboveThreshold() {
            // The documented way to force the compensation path: total > threshold.
            ProcessPaymentCommand command = processCommand(new BigDecimal("1200.00"));
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            CommandOutcome outcome = paymentService.processPayment(command);

            assertThat(outcome).isEqualTo(CommandOutcome.PAYMENT_FAILED);

            ArgumentCaptor<Payment> paymentCaptor = ArgumentCaptor.forClass(Payment.class);
            verify(paymentRepository).save(paymentCaptor.capture());
            assertThat(paymentCaptor.getValue().getStatus()).isEqualTo(PaymentStatus.FAILED);

            ArgumentCaptor<PaymentFailedEvent> eventCaptor = ArgumentCaptor.forClass(PaymentFailedEvent.class);
            verify(eventPublisher).publishPaymentFailed(eventCaptor.capture());
            assertThat(eventCaptor.getValue().reason())
                    .isEqualTo(PaymentFailureReason.AMOUNT_ABOVE_THRESHOLD);
            verify(eventPublisher, never()).publishPaymentCompleted(any(PaymentCompletedEvent.class));
        }

        @Test
        @DisplayName("a penny above the threshold is enough to fail")
        void justAboveThresholdFails() {
            ProcessPaymentCommand command = processCommand(new BigDecimal("1000.01"));
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            assertThat(paymentService.processPayment(command)).isEqualTo(CommandOutcome.PAYMENT_FAILED);
        }

        @Test
        @DisplayName("a zero threshold makes every payment fail, for exercising compensation")
        void zeroThresholdFailsEverything() {
            PaymentSimulationProperties alwaysFail = new PaymentSimulationProperties();
            alwaysFail.setFailureThreshold(BigDecimal.ZERO);
            PaymentService service = new PaymentService(
                    paymentRepository, processedCommandRepository, eventPublisher, alwaysFail);

            ProcessPaymentCommand command = processCommand(new BigDecimal("0.01"));
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            assertThat(service.processPayment(command)).isEqualTo(CommandOutcome.PAYMENT_FAILED);
        }
    }

    @Nested
    @DisplayName("refund reverses a payment")
    class Refund {

        @Test
        @DisplayName("a successful payment becomes REFUNDED")
        void refundsSuccessfulPayment() {
            UUID orderId = UUID.randomUUID();
            Payment payment = Payment.succeeded(UUID.randomUUID(), orderId, USER_ID, new BigDecimal("99.98"));
            RefundPaymentCommand command = refundCommand(orderId);

            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.SUCCEEDED))
                    .thenReturn(Optional.of(payment));
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            CommandOutcome outcome = paymentService.refundPayment(command);

            assertThat(outcome).isEqualTo(CommandOutcome.REFUNDED);
            assertThat(payment.getStatus()).isEqualTo(PaymentStatus.REFUNDED);
            // The amount is untouched: the row records what was taken, and therefore what
            // was given back.
            assertThat(payment.getAmount()).isEqualByComparingTo("99.98");
        }

        @Test
        @DisplayName("a refund with no successful payment is a logged no-op, not an error")
        void noRefundablePayment() {
            UUID orderId = UUID.randomUUID();
            RefundPaymentCommand command = refundCommand(orderId);
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.findByOrderIdAndStatus(orderId, PaymentStatus.SUCCEEDED))
                    .thenReturn(Optional.empty());

            assertThat(paymentService.refundPayment(command))
                    .isEqualTo(CommandOutcome.NO_REFUNDABLE_PAYMENT);
            verify(paymentRepository, never()).save(any(Payment.class));
        }

        @Test
        @DisplayName("a FAILED payment cannot be refunded — that would invent money never taken")
        void cannotRefundFailedPayment() {
            Payment failed = Payment.failed(UUID.randomUUID(), UUID.randomUUID(), USER_ID, new BigDecimal("1200.00"));

            assertThatThrownBy(failed::refund)
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("only SUCCEEDED can be refunded");
        }

        @Test
        @DisplayName("an already REFUNDED payment cannot be refunded again")
        void cannotRefundTwice() {
            Payment payment = Payment.succeeded(UUID.randomUUID(), UUID.randomUUID(), USER_ID, new BigDecimal("50.00"));
            payment.refund();

            assertThatThrownBy(payment::refund).isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    @DisplayName("idempotency — a redelivered command must not double-charge")
    class Idempotency {

        @Test
        @DisplayName("a redelivered ProcessPayment charges nothing and publishes nothing")
        void redeliveredProcessPaymentIsIgnored() {
            ProcessPaymentCommand command = processCommand(new BigDecimal("99.98"));
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(true);

            CommandOutcome outcome = paymentService.processPayment(command);

            assertThat(outcome).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            verifyNoInteractions(paymentRepository);
            verifyNoInteractions(eventPublisher);
            verify(processedCommandRepository, never()).save(any(ProcessedCommand.class));
        }

        @Test
        @DisplayName("delivering the same ProcessPayment twice charges the customer exactly once")
        void sameCommandTwiceChargesOnce() {
            ProcessPaymentCommand command = processCommand(new BigDecimal("99.98"));

            // First delivery: unseen. Second: the marker the first one wrote.
            when(processedCommandRepository.existsById(command.messageId()))
                    .thenReturn(false)
                    .thenReturn(true);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            CommandOutcome first = paymentService.processPayment(command);
            CommandOutcome second = paymentService.processPayment(command);

            assertThat(first).isEqualTo(CommandOutcome.PAID);
            assertThat(second).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);

            // One payment row, one event — never two of either.
            verify(paymentRepository, org.mockito.Mockito.times(1)).save(any(Payment.class));
            verify(eventPublisher, org.mockito.Mockito.times(1))
                    .publishPaymentCompleted(any(PaymentCompletedEvent.class));
        }

        @Test
        @DisplayName("a redelivered RefundPayment does not refund twice")
        void redeliveredRefundIsIgnored() {
            RefundPaymentCommand command = refundCommand(UUID.randomUUID());
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(true);

            assertThat(paymentService.refundPayment(command)).isEqualTo(CommandOutcome.DUPLICATE_IGNORED);
            verifyNoInteractions(paymentRepository);
        }

        @Test
        @DisplayName("a failed payment is also marked processed, so replaying it cannot later succeed")
        void failedPaymentIsMarkedProcessed() {
            ProcessPaymentCommand command = processCommand(new BigDecimal("1200.00"));
            when(processedCommandRepository.existsById(command.messageId())).thenReturn(false);
            when(paymentRepository.save(any(Payment.class))).thenAnswer(inv -> inv.getArgument(0));

            paymentService.processPayment(command);

            ArgumentCaptor<ProcessedCommand> captor = ArgumentCaptor.forClass(ProcessedCommand.class);
            verify(processedCommandRepository).save(captor.capture());
            assertThat(captor.getValue().getMessageId()).isEqualTo(command.messageId());
            assertThat(captor.getValue().getCommandType()).isEqualTo("ProcessPayment");
        }
    }
}
