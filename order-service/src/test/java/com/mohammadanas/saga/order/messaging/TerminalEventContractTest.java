package com.mohammadanas.saga.order.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the JSON field names of the two terminal-state events, against
 * notification-service's records.
 *
 * <p>This is the mirror of saga-orchestrator's {@code OutboundContractTest}, and it exists
 * for the same reason: the wire is JSON resolved against the <em>consumer's</em> declared
 * type, with type headers deliberately disabled on both sides. A field renamed here does
 * not fail to compile, does not fail any behavioural test in this module, and does not
 * error at notification-service either — it simply arrives there as {@code null}, and a
 * customer gets "Your order for null (null) is confirmed." No other test in the repository
 * would catch that, so the names below are spelled out literally rather than derived from
 * the record.
 *
 * <p>The expected sets were taken from notification-service's {@code OrderConfirmedEvent}
 * and {@code OrderCancelledEvent}. They are duplicated as strings because the two modules
 * share no code — see ARCHITECTURE.md section 7 on {@code common-contracts} being a
 * deliberately deferred escape hatch. Duplicating the names is the cost of that choice;
 * this test is what stops the duplication drifting silently.
 */
class TerminalEventContractTest {

    /**
     * Configured like the Spring Boot mapper on both sides: JSR-310 registered, so
     * {@code Instant} is writable at all.
     */
    private static final ObjectMapper MAPPER =
            JsonMapper.builder().addModule(new JavaTimeModule()).build();

    /** Exactly the field set notification-service declares, for both events. */
    private static final Set<String> EXPECTED_FIELDS =
            Set.of("messageId", "sagaId", "orderId", "userId", "item", "amount", "occurredAt");

    private static final UUID SAGA_ID = UUID.randomUUID();
    private static final UUID ORDER_ID = UUID.randomUUID();
    private static final BigDecimal AMOUNT = new BigDecimal("99.98");

    private static Set<String> fieldsOf(Object payload) throws Exception {
        return MAPPER.readTree(MAPPER.writeValueAsString(payload)).properties().stream()
                .map(java.util.Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    @Test
    @DisplayName("OrderConfirmed matches notification-service's OrderConfirmedEvent field for field")
    void orderConfirmedMatchesConsumer() throws Exception {
        OrderConfirmedEvent event = OrderConfirmedEvent.from(
                SAGA_ID, ORDER_ID, "user-42", "Mechanical keyboard", AMOUNT);

        // containsExactlyInAnyOrder, not contains: an extra field is a contract change too.
        assertThat(fieldsOf(event)).containsExactlyInAnyOrderElementsOf(EXPECTED_FIELDS);
    }

    @Test
    @DisplayName("OrderCancelled matches notification-service's OrderCancelledEvent field for field")
    void orderCancelledMatchesConsumer() throws Exception {
        OrderCancelledEvent event = OrderCancelledEvent.from(
                SAGA_ID, ORDER_ID, "user-42", "Mechanical keyboard", AMOUNT);

        assertThat(fieldsOf(event)).containsExactlyInAnyOrderElementsOf(EXPECTED_FIELDS);
    }

    @Test
    @DisplayName("OrderCancelled carries no failure reason — the consumer has no field for one")
    void orderCancelledCarriesNoReason() throws Exception {
        // notification-service's record deliberately omits a reason: the customer message
        // does not vary by cause, and order-service could not supply one anyway (it holds
        // no saga state). Adding one here would be a field with no reader.
        assertThat(fieldsOf(OrderCancelledEvent.from(SAGA_ID, ORDER_ID, "user-9", "Desk mat", AMOUNT)))
                .doesNotContain("reason", "failureReason", "cause");
    }

    @Test
    @DisplayName("the values a customer sees survive serialization: userId, item and amount")
    void payloadValuesSurviveSerialization() throws Exception {
        OrderConfirmedEvent event = OrderConfirmedEvent.from(
                SAGA_ID, ORDER_ID, "user-42", "Mechanical keyboard", AMOUNT);

        JsonNode json = MAPPER.readTree(MAPPER.writeValueAsString(event));

        assertThat(json.get("sagaId").asText()).isEqualTo(SAGA_ID.toString());
        assertThat(json.get("orderId").asText()).isEqualTo(ORDER_ID.toString());
        assertThat(json.get("userId").asText()).isEqualTo("user-42");
        assertThat(json.get("item").asText()).isEqualTo("Mechanical keyboard");
        assertThat(json.get("messageId").asText()).isNotBlank();
        assertThat(json.get("occurredAt").isNull()).isFalse();

        // The money value specifically: notification-service renders it straight into the
        // customer's message, so it must not arrive as a float that prints as 99.97999.
        assertThat(new BigDecimal(json.get("amount").asText())).isEqualByComparingTo(AMOUNT);
    }

    @Test
    @DisplayName("the topic names match notification-service's NotificationTopics")
    void topicNamesMatchConsumer() {
        // Duplicated as literals for the same reason as the field names: a typo here is a
        // consumer subscribed to a topic nobody publishes to, and nothing anywhere fails.
        assertThat(OrderTopics.ORDER_CONFIRMED).isEqualTo("order.events.order-confirmed.v1");
        assertThat(OrderTopics.ORDER_CANCELLED).isEqualTo("order.events.order-cancelled.v1");
    }
}
