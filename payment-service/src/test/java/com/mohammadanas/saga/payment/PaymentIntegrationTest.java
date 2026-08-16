package com.mohammadanas.saga.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mohammadanas.saga.payment.domain.Payment;
import com.mohammadanas.saga.payment.domain.PaymentRepository;
import com.mohammadanas.saga.payment.domain.PaymentStatus;
import com.mohammadanas.saga.payment.domain.ProcessedCommandRepository;
import com.mohammadanas.saga.payment.messaging.CompensationOutcome;
import com.mohammadanas.saga.payment.messaging.PaymentCompletedEvent;
import com.mohammadanas.saga.payment.messaging.PaymentFailedEvent;
import com.mohammadanas.saga.payment.messaging.PaymentFailureReason;
import com.mohammadanas.saga.payment.messaging.PaymentRefundedEvent;
import com.mohammadanas.saga.payment.messaging.PaymentTopics;
import com.mohammadanas.saga.payment.messaging.ProcessPaymentCommand;
import com.mohammadanas.saga.payment.messaging.RefundPaymentCommand;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end tests against real Postgres and Kafka (ARCHITECTURE.md section 8.2).
 *
 * <p>This is where the idempotency guarantee is actually proven: the unit tests stub the
 * dedup lookup, so only a real database enforces the {@code processed_commands} primary
 * key that makes a redelivered ProcessPayment incapable of charging twice.
 *
 * <p><strong>These tests SKIP when Testcontainers cannot reach a Docker daemon.</strong>
 * A green build does not on its own prove they ran — check the surefire output for skips.
 * As of this commit they have never executed on the development machine; see README.
 */
@SpringBootTest(properties = "payment.simulation.failure-threshold=1000.00")
@Testcontainers(disabledWithoutDocker = true)
class PaymentIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    private static Consumer<String, String> eventConsumer;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private ProcessedCommandRepository processedCommandRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Built here rather than autowired — payment-service has no
     * {@code spring-boot-starter-web}, so there is no auto-configured
     * {@code ObjectMapper} bean. See the same note on inventory-service's integration test:
     * this failed at context load from the day it was written and was invisible while the
     * class was being skipped.
     */
    private static final ObjectMapper objectMapper =
            JsonMapper.builder().addModule(new JavaTimeModule()).build();

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    /** Command topics this service consumes but does not own. */
    @TestConfiguration
    static class CommandTopicsConfiguration {

        @Bean
        NewTopic processPaymentTopic() {
            return TopicBuilder.name(PaymentTopics.PROCESS_PAYMENT).partitions(1).replicas(1).build();
        }

        @Bean
        NewTopic refundPaymentTopic() {
            return TopicBuilder.name(PaymentTopics.REFUND_PAYMENT).partitions(1).replicas(1).build();
        }
    }

    @BeforeEach
    void setUp() {
        paymentRepository.deleteAll();
        processedCommandRepository.deleteAll();

        if (eventConsumer == null) {
            Map<String, Object> props = new HashMap<>(
                    KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), "payment-test-consumer", "true"));
            props.put("key.deserializer", StringDeserializer.class);
            props.put("value.deserializer", StringDeserializer.class);
            props.put("auto.offset.reset", "earliest");
            eventConsumer = new KafkaConsumer<>(props);
            eventConsumer.subscribe(List.of(
                    PaymentTopics.PAYMENT_COMPLETED,
                    PaymentTopics.PAYMENT_FAILED,
                    PaymentTopics.PAYMENT_REFUNDED));
        }
    }

    @AfterAll
    static void closeConsumer() {
        if (eventConsumer != null) {
            eventConsumer.close();
            eventConsumer = null;
        }
    }

    @Test
    @DisplayName("a payment below the threshold succeeds and publishes PaymentCompleted")
    void successfulPayment() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();

        kafkaTemplate.send(PaymentTopics.PROCESS_PAYMENT, sagaId.toString(),
                new ProcessPaymentCommand(UUID.randomUUID(), sagaId, orderId, "user-42", new BigDecimal("99.98")));

        awaitPaymentStatus(orderId, PaymentStatus.SUCCEEDED);

        PaymentCompletedEvent event = awaitEvent(
                PaymentTopics.PAYMENT_COMPLETED, PaymentCompletedEvent.class, e -> e.orderId().equals(orderId));
        assertThat(event.amount()).isEqualByComparingTo("99.98");
    }

    @Test
    @DisplayName("a payment above the threshold fails with AMOUNT_ABOVE_THRESHOLD")
    void failedPaymentAboveThreshold() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();

        // The documented trigger: total above the configured threshold.
        kafkaTemplate.send(PaymentTopics.PROCESS_PAYMENT, sagaId.toString(),
                new ProcessPaymentCommand(UUID.randomUUID(), sagaId, orderId, "user-42", new BigDecimal("1200.00")));

        awaitPaymentStatus(orderId, PaymentStatus.FAILED);

        PaymentFailedEvent event = awaitEvent(
                PaymentTopics.PAYMENT_FAILED, PaymentFailedEvent.class, e -> e.orderId().equals(orderId));
        assertThat(event.reason()).isEqualTo(PaymentFailureReason.AMOUNT_ABOVE_THRESHOLD);
    }

    @Test
    @DisplayName("a RefundPayment command reverses a successful payment")
    void refundReversesPayment() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();

        kafkaTemplate.send(PaymentTopics.PROCESS_PAYMENT, sagaId.toString(),
                new ProcessPaymentCommand(UUID.randomUUID(), sagaId, orderId, "user-42", new BigDecimal("250.00")));
        awaitPaymentStatus(orderId, PaymentStatus.SUCCEEDED);

        kafkaTemplate.send(PaymentTopics.REFUND_PAYMENT, sagaId.toString(),
                new RefundPaymentCommand(UUID.randomUUID(), sagaId, orderId));

        awaitPaymentStatus(orderId, PaymentStatus.REFUNDED);

        // The amount is preserved: the row records what was taken and therefore what was
        // handed back.
        Payment payment = paymentRepository.findAll().stream()
                .filter(p -> p.getOrderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        assertThat(payment.getAmount()).isEqualByComparingTo("250.00");
    }

    @Test
    @DisplayName("refunding an order that never paid still confirms compensation")
    void refundWithNothingPaidStillConfirms() throws Exception {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();

        kafkaTemplate.send(PaymentTopics.REFUND_PAYMENT, sagaId.toString(),
                new RefundPaymentCommand(UUID.randomUUID(), sagaId, orderId));

        PaymentRefundedEvent event = awaitEvent(
                PaymentTopics.PAYMENT_REFUNDED, PaymentRefundedEvent.class,
                e -> e.orderId().equals(orderId));

        assertThat(event.outcome()).isEqualTo(CompensationOutcome.NOTHING_TO_REVERSE);
        assertThat(event.paymentId()).isNull();
        assertThat(event.amount()).isNull();

        // No payment row invented by the acknowledgement.
        assertThat(paymentRepository.findAll())
                .filteredOn(p -> p.getOrderId().equals(orderId))
                .isEmpty();
    }

    @Test
    @DisplayName("a redelivered ProcessPayment charges exactly once, enforced by the database")
    void redeliveredCommandDoesNotDoubleCharge() {
        UUID orderId = UUID.randomUUID();
        UUID sagaId = UUID.randomUUID();
        ProcessPaymentCommand command = new ProcessPaymentCommand(
                UUID.randomUUID(), sagaId, orderId, "user-42", new BigDecimal("99.98"));

        kafkaTemplate.send(PaymentTopics.PROCESS_PAYMENT, sagaId.toString(), command);
        awaitPaymentStatus(orderId, PaymentStatus.SUCCEEDED);

        // The identical message again, exactly as an at-least-once broker would replay it.
        kafkaTemplate.send(PaymentTopics.PROCESS_PAYMENT, sagaId.toString(), command);

        // A follow-up command for a different order acts as a barrier: once it has been
        // applied, the duplicate ahead of it on the same partition has certainly been
        // handled, so the assertion below is not racing the consumer.
        UUID otherOrderId = UUID.randomUUID();
        kafkaTemplate.send(PaymentTopics.PROCESS_PAYMENT, sagaId.toString(),
                new ProcessPaymentCommand(UUID.randomUUID(), sagaId, otherOrderId, "user-42", new BigDecimal("10.00")));
        awaitPaymentStatus(otherOrderId, PaymentStatus.SUCCEEDED);

        // Exactly one payment row for the replayed order — never two.
        assertThat(paymentRepository.findAll())
                .filteredOn(p -> p.getOrderId().equals(orderId))
                .hasSize(1);
    }

    private void awaitPaymentStatus(UUID orderId, PaymentStatus expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted(() ->
                assertThat(paymentRepository.findByOrderIdAndStatus(orderId, expected)).isPresent());
    }

    private <T> T awaitEvent(String topic, Class<T> type, Predicate<T> matcher) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = eventConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                if (!record.topic().equals(topic)) {
                    continue;
                }
                T event = objectMapper.readValue(record.value(), type);
                if (matcher.test(event)) {
                    return event;
                }
            }
        }
        throw new AssertionError("No matching event observed on " + topic);
    }
}
