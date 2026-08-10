package com.mohammadanas.saga.inventory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohammadanas.saga.inventory.domain.Inventory;
import com.mohammadanas.saga.inventory.domain.InventoryRepository;
import com.mohammadanas.saga.inventory.domain.ProcessedCommandRepository;
import com.mohammadanas.saga.inventory.domain.ReservationRepository;
import com.mohammadanas.saga.inventory.domain.ReservationStatus;
import com.mohammadanas.saga.inventory.messaging.InventoryReservationFailedEvent;
import com.mohammadanas.saga.inventory.messaging.InventoryReservedEvent;
import com.mohammadanas.saga.inventory.messaging.InventoryTopics;
import com.mohammadanas.saga.inventory.messaging.ReleaseInventoryCommand;
import com.mohammadanas.saga.inventory.messaging.ReservationFailureReason;
import com.mohammadanas.saga.inventory.messaging.ReserveInventoryCommand;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
 * <p>This is where the idempotency guarantee is actually proven. The unit tests stub the
 * dedup lookup, so they demonstrate the branch but not the mechanism; only a real
 * database enforces the {@code processed_commands} primary key that makes a redelivered
 * command incapable of reserving twice.
 *
 * <p><strong>These tests SKIP when Testcontainers cannot reach a Docker daemon.</strong>
 * A green build does not on its own prove they ran — check the surefire output for skips.
 * As of this commit they have never executed on the development machine; see README.
 */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class InventoryIntegrationTest {

    private static final String ITEM = "MECH-KB-01";

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    private static Consumer<String, String> eventConsumer;

    @Autowired
    private InventoryRepository inventoryRepository;

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private ProcessedCommandRepository processedCommandRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

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
        NewTopic reserveInventoryTopic() {
            return TopicBuilder.name(InventoryTopics.RESERVE_INVENTORY).partitions(1).replicas(1).build();
        }

        @Bean
        NewTopic releaseInventoryTopic() {
            return TopicBuilder.name(InventoryTopics.RELEASE_INVENTORY).partitions(1).replicas(1).build();
        }
    }

    @BeforeEach
    void resetStock() {
        reservationRepository.deleteAll();
        processedCommandRepository.deleteAll();
        inventoryRepository.deleteAll();
        inventoryRepository.save(new Inventory(ITEM, 10));

        if (eventConsumer == null) {
            Map<String, Object> props = new HashMap<>(
                    KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), "inventory-test-consumer", "true"));
            props.put("key.deserializer", StringDeserializer.class);
            props.put("value.deserializer", StringDeserializer.class);
            props.put("auto.offset.reset", "earliest");
            eventConsumer = new KafkaConsumer<>(props);
            eventConsumer.subscribe(List.of(
                    InventoryTopics.INVENTORY_RESERVED,
                    InventoryTopics.INVENTORY_RESERVATION_FAILED,
                    InventoryTopics.INVENTORY_RELEASED));
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
    @DisplayName("a ReserveInventory command reserves stock and publishes InventoryReserved")
    void reservesStock() throws Exception {
        UUID orderId = UUID.randomUUID();
        kafkaTemplate.send(InventoryTopics.RESERVE_INVENTORY, orderId.toString(),
                new ReserveInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), orderId, ITEM, 3));

        awaitStock(7, 3);

        InventoryReservedEvent event = awaitEvent(
                InventoryTopics.INVENTORY_RESERVED, InventoryReservedEvent.class, e -> e.orderId().equals(orderId));
        assertThat(event.quantity()).isEqualTo(3);
    }

    @Test
    @DisplayName("insufficient stock publishes InventoryReservationFailed and changes nothing")
    void insufficientStock() throws Exception {
        UUID orderId = UUID.randomUUID();
        kafkaTemplate.send(InventoryTopics.RESERVE_INVENTORY, orderId.toString(),
                new ReserveInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), orderId, ITEM, 99));

        InventoryReservationFailedEvent event = awaitEvent(
                InventoryTopics.INVENTORY_RESERVATION_FAILED,
                InventoryReservationFailedEvent.class,
                e -> e.orderId().equals(orderId));

        assertThat(event.reason()).isEqualTo(ReservationFailureReason.INSUFFICIENT_STOCK);
        Inventory inventory = inventoryRepository.findById(ITEM).orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(10);
        assertThat(inventory.getReservedQuantity()).isZero();
    }

    @Test
    @DisplayName("a ReleaseInventory command reverses the reservation exactly")
    void releaseReversesReservation() {
        UUID orderId = UUID.randomUUID();
        kafkaTemplate.send(InventoryTopics.RESERVE_INVENTORY, orderId.toString(),
                new ReserveInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), orderId, ITEM, 4));
        awaitStock(6, 4);

        kafkaTemplate.send(InventoryTopics.RELEASE_INVENTORY, orderId.toString(),
                new ReleaseInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), orderId));

        // Stock returns exactly to its pre-saga level: the invariant that makes this a
        // true undo rather than an approximate one.
        awaitStock(10, 0);
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(reservationRepository.findByOrderIdAndStatus(orderId, ReservationStatus.RESERVED))
                        .isEmpty());
    }

    @Test
    @DisplayName("a redelivered ReserveInventory reserves exactly once, enforced by the database")
    void redeliveredCommandDoesNotDoubleReserve() {
        UUID orderId = UUID.randomUUID();
        ReserveInventoryCommand command =
                new ReserveInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), orderId, ITEM, 3);

        kafkaTemplate.send(InventoryTopics.RESERVE_INVENTORY, orderId.toString(), command);
        awaitStock(7, 3);

        // The identical message again, exactly as an at-least-once broker would replay it.
        kafkaTemplate.send(InventoryTopics.RESERVE_INVENTORY, orderId.toString(), command);

        // A follow-up command for a different order acts as a barrier: once it has been
        // applied, the duplicate ahead of it on the same partition has certainly been
        // handled, so the assertion below is not just racing the consumer.
        UUID otherOrderId = UUID.randomUUID();
        kafkaTemplate.send(InventoryTopics.RESERVE_INVENTORY, otherOrderId.toString(),
                new ReserveInventoryCommand(UUID.randomUUID(), UUID.randomUUID(), otherOrderId, ITEM, 1));
        awaitStock(6, 4);

        // 3 + 1 units taken, never 3 + 3 + 1.
        Inventory inventory = inventoryRepository.findById(ITEM).orElseThrow();
        assertThat(inventory.getAvailableQuantity()).isEqualTo(6);
        assertThat(inventory.getReservedQuantity()).isEqualTo(4);
        assertThat(reservationRepository.findAll())
                .filteredOn(r -> r.getOrderId().equals(orderId))
                .hasSize(1);
    }

    private void awaitStock(int expectedAvailable, int expectedReserved) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted(() -> {
            Inventory inventory = inventoryRepository.findById(ITEM).orElseThrow();
            assertThat(inventory.getAvailableQuantity()).isEqualTo(expectedAvailable);
            assertThat(inventory.getReservedQuantity()).isEqualTo(expectedReserved);
        });
    }

    private <T> T awaitEvent(String topic, Class<T> type, java.util.function.Predicate<T> matcher) throws Exception {
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
