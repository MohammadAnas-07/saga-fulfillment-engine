package com.mohammadanas.saga.notification;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.mohammadanas.saga.notification.domain.ProcessedMessageRepository;
import com.mohammadanas.saga.notification.messaging.NotificationTopics;
import com.mohammadanas.saga.notification.messaging.OrderCancelledEvent;
import com.mohammadanas.saga.notification.messaging.OrderConfirmedEvent;
import com.mohammadanas.saga.notification.service.Notification;
import com.mohammadanas.saga.notification.service.NotificationSender;
import com.mohammadanas.saga.notification.service.NotificationType;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
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
 * dedup lookup, so only a real database enforces the {@code processed_messages} primary
 * key that stops a redelivered event becoming a second message to the customer.
 *
 * <p>The recording sender below replaces the logging one — it is a test double for the
 * <em>delivery channel</em>, not for any infrastructure. Kafka and Postgres are real.
 *
 * <p><strong>These tests SKIP when Testcontainers cannot reach a Docker daemon.</strong>
 * A green build does not on its own prove they ran — check the surefire output for skips.
 * As of this commit they have never executed on the development machine; see README.
 */
@SpringBootTest(properties = {
        // notification-service publishes nothing in production, so it configures no
        // producer. These exist only so the test harness can stand in for order-service
        // and put events on the topics under test.
        "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer",
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer",
        "spring.kafka.producer.properties.spring.json.add.type.headers=false"
})
@Testcontainers(disabledWithoutDocker = true)
class NotificationIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    @Autowired
    private ProcessedMessageRepository processedMessageRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RecordingNotificationSender sender;

    @DynamicPropertySource
    static void containerProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    /** Captures what would have been delivered, and declares the topics we publish into. */
    @TestConfiguration
    static class TestConfig {

        @Bean
        @Primary
        RecordingNotificationSender recordingNotificationSender() {
            return new RecordingNotificationSender();
        }

        @Bean
        NewTopic orderConfirmedTopic() {
            return TopicBuilder.name(NotificationTopics.ORDER_CONFIRMED).partitions(1).replicas(1).build();
        }

        @Bean
        NewTopic orderCancelledTopic() {
            return TopicBuilder.name(NotificationTopics.ORDER_CANCELLED).partitions(1).replicas(1).build();
        }
    }

    static class RecordingNotificationSender implements NotificationSender {

        private final List<Notification> sent = new CopyOnWriteArrayList<>();

        @Override
        public void send(Notification notification) {
            sent.add(notification);
        }

        List<Notification> sent() {
            return sent;
        }

        void clear() {
            sent.clear();
        }
    }

    @BeforeEach
    void setUp() {
        sender.clear();
        processedMessageRepository.deleteAll();
    }

    @Test
    @DisplayName("an OrderConfirmed event produces a confirmation notification")
    void confirmedProducesNotification() {
        UUID orderId = UUID.randomUUID();
        kafkaTemplate.send(NotificationTopics.ORDER_CONFIRMED, orderId.toString(),
                new OrderConfirmedEvent(UUID.randomUUID(), UUID.randomUUID(), orderId,
                        "user-42", "Mechanical keyboard", new BigDecimal("99.98"), Instant.now()));

        awaitNotification(orderId, NotificationType.ORDER_CONFIRMED);

        Notification notification = sender.sent().stream()
                .filter(n -> n.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        assertThat(notification.userId()).isEqualTo("user-42");
        assertThat(notification.message()).contains("is confirmed");
    }

    @Test
    @DisplayName("an OrderCancelled event produces a cancellation notification")
    void cancelledProducesNotification() {
        UUID orderId = UUID.randomUUID();
        kafkaTemplate.send(NotificationTopics.ORDER_CANCELLED, orderId.toString(),
                new OrderCancelledEvent(UUID.randomUUID(), UUID.randomUUID(), orderId,
                        "user-7", "Desk mat", new BigDecimal("19.00"), Instant.now()));

        awaitNotification(orderId, NotificationType.ORDER_CANCELLED);

        Notification notification = sender.sent().stream()
                .filter(n -> n.orderId().equals(orderId))
                .findFirst()
                .orElseThrow();
        assertThat(notification.message()).contains("was cancelled");
    }

    @Test
    @DisplayName("a redelivered event notifies exactly once, enforced by the database")
    void redeliveredEventNotifiesOnce() {
        UUID orderId = UUID.randomUUID();
        OrderConfirmedEvent event = new OrderConfirmedEvent(
                UUID.randomUUID(), UUID.randomUUID(), orderId,
                "user-42", "Mechanical keyboard", new BigDecimal("99.98"), Instant.now());

        kafkaTemplate.send(NotificationTopics.ORDER_CONFIRMED, orderId.toString(), event);
        awaitNotification(orderId, NotificationType.ORDER_CONFIRMED);

        // The identical message again, exactly as an at-least-once broker would replay it.
        kafkaTemplate.send(NotificationTopics.ORDER_CONFIRMED, orderId.toString(), event);

        // A follow-up event for a different order acts as a barrier: once it has been
        // handled, the duplicate ahead of it on the same partition certainly has been too.
        UUID otherOrderId = UUID.randomUUID();
        kafkaTemplate.send(NotificationTopics.ORDER_CONFIRMED, orderId.toString(),
                new OrderConfirmedEvent(UUID.randomUUID(), UUID.randomUUID(), otherOrderId,
                        "user-42", "Lamp", new BigDecimal("30.00"), Instant.now()));
        awaitNotification(otherOrderId, NotificationType.ORDER_CONFIRMED);

        assertThat(sender.sent()).filteredOn(n -> n.orderId().equals(orderId)).hasSize(1);
    }

    private void awaitNotification(UUID orderId, NotificationType type) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted(() ->
                assertThat(sender.sent())
                        .anyMatch(n -> n.orderId().equals(orderId) && n.type() == type));
    }
}
