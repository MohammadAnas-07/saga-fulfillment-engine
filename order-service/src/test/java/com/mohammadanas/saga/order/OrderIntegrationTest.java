package com.mohammadanas.saga.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mohammadanas.saga.order.api.CreateOrderRequest;
import com.mohammadanas.saga.order.api.OrderResponse;
import com.mohammadanas.saga.order.domain.Order;
import com.mohammadanas.saga.order.domain.OrderRepository;
import com.mohammadanas.saga.order.domain.OrderStatus;
import com.mohammadanas.saga.order.messaging.CancelOrderCommand;
import com.mohammadanas.saga.order.messaging.ConfirmOrderCommand;
import com.mohammadanas.saga.order.messaging.OrderCreatedEvent;
import com.mohammadanas.saga.order.messaging.OrderTopics;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.apache.kafka.clients.admin.NewTopic;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * End-to-end tests against a real Postgres and a real Kafka broker (ARCHITECTURE.md
 * section 8.2 — no embedded fakes, no mocked brokers).
 *
 * <p>Covers the REST entry point, the event actually reaching the wire, and the command
 * listeners actually driving status changes. The unit tests in
 * {@code OrderServiceTest} cover the transition rules themselves; this class proves the
 * wiring around them is real.
 *
 * <p><strong>These tests SKIP when Testcontainers cannot reach a Docker daemon.</strong>
 * A green build therefore does not on its own prove they ran — check the surefire output
 * for skips before trusting it. As of this commit they have never executed successfully
 * on the development machine: Docker Desktop's engine (API 1.55) returns a stubbed
 * {@code 400} on {@code /info} to docker-java, so Testcontainers rejects the environment
 * even though the {@code docker} CLI works. See README "Running the tests".
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers(disabledWithoutDocker = true)
class OrderIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:3.8.0"));

    private static Consumer<String, String> orderCreatedConsumer;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private OrderRepository orderRepository;

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

    /**
     * Declares the command topics order-service consumes but does not own, so the test
     * does not depend on broker auto-creation timing.
     */
    @TestConfiguration
    static class CommandTopicsConfiguration {

        @Bean
        NewTopic confirmOrderTopic() {
            return TopicBuilder.name(OrderTopics.CONFIRM_ORDER).partitions(1).replicas(1).build();
        }

        @Bean
        NewTopic cancelOrderTopic() {
            return TopicBuilder.name(OrderTopics.CANCEL_ORDER).partitions(1).replicas(1).build();
        }
    }

    @BeforeAll
    static void startConsumer() {
        Map<String, Object> props = new HashMap<>(
                KafkaTestUtils.consumerProps(KAFKA.getBootstrapServers(), "order-created-test-consumer", "true"));
        props.put("key.deserializer", StringDeserializer.class);
        props.put("value.deserializer", StringDeserializer.class);
        props.put("auto.offset.reset", "earliest");

        orderCreatedConsumer = new org.apache.kafka.clients.consumer.KafkaConsumer<>(props);
        orderCreatedConsumer.subscribe(java.util.List.of(OrderTopics.ORDER_CREATED));
    }

    @AfterAll
    static void stopConsumer() {
        if (orderCreatedConsumer != null) {
            orderCreatedConsumer.close();
        }
    }

    @Test
    @DisplayName("POST /orders persists a PENDING order and publishes OrderCreated to Kafka")
    void createOrderPersistsAndPublishes() throws Exception {
        CreateOrderRequest request = new CreateOrderRequest(
                "user-42", "MECH-KB-01", "Mechanical keyboard", 2, new BigDecimal("49.99"));

        ResponseEntity<OrderResponse> response =
                restTemplate.postForEntity("/orders", request, OrderResponse.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        OrderResponse body = response.getBody();
        assertThat(body).isNotNull();
        assertThat(body.status()).isEqualTo(OrderStatus.PENDING);

        Order persisted = orderRepository.findById(body.id()).orElseThrow();
        assertThat(persisted.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(persisted.getUserId()).isEqualTo("user-42");
        assertThat(persisted.getItemSku()).isEqualTo("MECH-KB-01");
        assertThat(persisted.getQuantity()).isEqualTo(2);
        assertThat(persisted.getCreatedAt()).isNotNull();

        OrderCreatedEvent event = awaitOrderCreated(body.id());
        assertThat(event.userId()).isEqualTo("user-42");
        assertThat(event.item()).isEqualTo("Mechanical keyboard");
        assertThat(event.amount()).isEqualByComparingTo(new BigDecimal("49.99"));
        assertThat(event.messageId()).isNotNull();
        assertThat(event.itemSku()).isEqualTo("MECH-KB-01");
        assertThat(event.quantity()).isEqualTo(2);
    }

    @Test
    @DisplayName("POST /orders rejects a missing itemSku or a quantity below 1")
    void rejectsInvalidRequests() {
        ResponseEntity<String> missingSku = restTemplate.postForEntity("/orders",
                new CreateOrderRequest("user-9", "  ", "Desk mat", 1, new BigDecimal("19.00")), String.class);
        assertThat(missingSku.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);

        ResponseEntity<String> zeroQuantity = restTemplate.postForEntity("/orders",
                new CreateOrderRequest("user-9", "DESK-MAT-01", "Desk mat", 0, new BigDecimal("19.00")), String.class);
        assertThat(zeroQuantity.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    @DisplayName("GET /orders/{id} returns the order, and 404 for an unknown id")
    void getOrder() {
        Order saved = orderRepository.save(
                Order.create("user-7", "DESK-MAT-01", "Desk mat", 1, new BigDecimal("19.00")));

        ResponseEntity<OrderResponse> found =
                restTemplate.getForEntity("/orders/" + saved.getId(), OrderResponse.class);
        assertThat(found.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(found.getBody()).isNotNull();
        assertThat(found.getBody().id()).isEqualTo(saved.getId());

        ResponseEntity<String> missing =
                restTemplate.getForEntity("/orders/" + UUID.randomUUID(), String.class);
        assertThat(missing.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    @DisplayName("a ConfirmOrder command from the orchestrator sets the order CONFIRMED")
    void confirmOrderCommandUpdatesStatus() {
        Order saved = orderRepository.save(
                Order.create("user-1", "MON-ARM-01", "Monitor arm", 1, new BigDecimal("89.00")));

        kafkaTemplate.send(OrderTopics.CONFIRM_ORDER, saved.getId().toString(),
                new ConfirmOrderCommand(UUID.randomUUID(), UUID.randomUUID(), saved.getId()));

        awaitStatus(saved.getId(), OrderStatus.CONFIRMED);
    }

    @Test
    @DisplayName("a CancelOrder command from the orchestrator sets the order CANCELLED")
    void cancelOrderCommandUpdatesStatus() {
        Order saved = orderRepository.save(
                Order.create("user-2", "USB-HUB-01", "USB hub", 1, new BigDecimal("25.50")));

        kafkaTemplate.send(OrderTopics.CANCEL_ORDER, saved.getId().toString(),
                new CancelOrderCommand(UUID.randomUUID(), UUID.randomUUID(), saved.getId()));

        awaitStatus(saved.getId(), OrderStatus.CANCELLED);
    }

    @Test
    @DisplayName("a redelivered ConfirmOrder is a no-op and does not kill the consumer")
    void redeliveredConfirmOrderIsIgnored() {
        Order saved = orderRepository.save(
                Order.create("user-3", "CBL-TRAY-01", "Cable tray", 1, new BigDecimal("15.00")));
        UUID sagaId = UUID.randomUUID();

        ConfirmOrderCommand command =
                new ConfirmOrderCommand(UUID.randomUUID(), sagaId, saved.getId());

        kafkaTemplate.send(OrderTopics.CONFIRM_ORDER, saved.getId().toString(), command);
        awaitStatus(saved.getId(), OrderStatus.CONFIRMED);

        // Exact same message again, as an at-least-once broker would redeliver it.
        kafkaTemplate.send(OrderTopics.CONFIRM_ORDER, saved.getId().toString(), command);

        // Still CONFIRMED, and — the real assertion — the listener is still alive
        // afterwards, proving the duplicate was not a poison pill.
        Order followUp = orderRepository.save(
                Order.create("user-3", "LAMP-01", "Lamp", 1, new BigDecimal("30.00")));
        kafkaTemplate.send(OrderTopics.CONFIRM_ORDER, followUp.getId().toString(),
                new ConfirmOrderCommand(UUID.randomUUID(), UUID.randomUUID(), followUp.getId()));

        awaitStatus(followUp.getId(), OrderStatus.CONFIRMED);
        assertThat(orderRepository.findById(saved.getId()).orElseThrow().getStatus())
                .isEqualTo(OrderStatus.CONFIRMED);
    }

    private void awaitStatus(UUID orderId, OrderStatus expected) {
        await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(250)).untilAsserted(() ->
                assertThat(orderRepository.findById(orderId).orElseThrow().getStatus()).isEqualTo(expected));
    }

    private OrderCreatedEvent awaitOrderCreated(UUID orderId) throws Exception {
        long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = orderCreatedConsumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                OrderCreatedEvent event = objectMapper.readValue(record.value(), OrderCreatedEvent.class);
                if (event.orderId().equals(orderId)) {
                    assertThat(record.key()).isEqualTo(orderId.toString());
                    return event;
                }
            }
        }
        throw new AssertionError("No OrderCreated event observed for order " + orderId);
    }
}
