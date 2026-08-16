package com.mohammadanas.saga.orchestrator.messaging;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the JSON field names of every message the orchestrator publishes.
 *
 * <p>These are wire contracts against four other services, and the wire is JSON resolved
 * against the consumer's declared record type. A renamed field here does not fail to
 * compile and does not fail any behavioural test in this module — it arrives at the
 * consumer as {@code null} and the saga quietly misbehaves. This is the only test in the
 * module that would catch that, so the field names below are deliberately spelled out
 * rather than derived.
 *
 * <p>Each expected set was taken from the consuming service's record: inventory-service's
 * {@code ReserveInventoryCommand}, payment-service's {@code ProcessPaymentCommand},
 * order-service's {@code ConfirmOrderCommand}, notification-service's
 * {@code OrderConfirmedEvent}, and so on.
 */
class OutboundContractTest {

    private static final ObjectMapper MAPPER =
            JsonMapper.builder().addModule(new JavaTimeModule()).build();

    private static final UUID ID = UUID.randomUUID();

    // No timestamp constant here, deliberately. Every message this class covers is a
    // *command*, and none of them carries an occurredAt — announcing facts is the owning
    // service's job (section 3.1), so the orchestrator publishes no events and has nothing
    // to stamp. A leftover NOW constant sat here unused until Chunk 11; it was never
    // usable, not merely unused.

    private static java.util.Set<String> fieldsOf(Object payload) throws Exception {
        return MAPPER.readTree(MAPPER.writeValueAsString(payload)).properties().stream()
                .map(java.util.Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
    }

    @Test
    @DisplayName("ReserveInventory matches inventory-service's ReserveInventoryCommand")
    void reserveInventory() throws Exception {
        assertThat(fieldsOf(new OutboundMessages.ReserveInventory(ID, ID, ID, "MECH-KB-01", 2)))
                .containsExactlyInAnyOrder("messageId", "sagaId", "orderId", "item", "quantity");
    }

    @Test
    @DisplayName("ReleaseInventory matches inventory-service's ReleaseInventoryCommand")
    void releaseInventory() throws Exception {
        assertThat(fieldsOf(new OutboundMessages.ReleaseInventory(ID, ID, ID)))
                .containsExactlyInAnyOrder("messageId", "sagaId", "orderId");
    }

    @Test
    @DisplayName("ProcessPayment matches payment-service's ProcessPaymentCommand")
    void processPayment() throws Exception {
        assertThat(fieldsOf(new OutboundMessages.ProcessPayment(ID, ID, ID, "user-42", new BigDecimal("99.98"))))
                .containsExactlyInAnyOrder("messageId", "sagaId", "orderId", "userId", "amount");
    }

    @Test
    @DisplayName("RefundPayment matches payment-service's RefundPaymentCommand")
    void refundPayment() throws Exception {
        assertThat(fieldsOf(new OutboundMessages.RefundPayment(ID, ID, ID)))
                .containsExactlyInAnyOrder("messageId", "sagaId", "orderId");
    }

    @Test
    @DisplayName("ConfirmOrder and CancelOrder match order-service's command records")
    void orderCommands() throws Exception {
        assertThat(fieldsOf(new OutboundMessages.ConfirmOrder(ID, ID, ID)))
                .containsExactlyInAnyOrder("messageId", "sagaId", "orderId");
        assertThat(fieldsOf(new OutboundMessages.CancelOrder(ID, ID, ID)))
                .containsExactlyInAnyOrder("messageId", "sagaId", "orderId");
    }

    @Test
    @DisplayName("the orchestrator declares no terminal-state events — those are order-service's")
    void publishesNoTerminalStateEvents() {
        // Guards the §3.1 boundary rather than a payload: the orchestrator issues
        // ConfirmOrder/CancelOrder and stops. If someone reintroduces an OrderConfirmed or
        // OrderCancelled record here, this fails and sends them to read §3.1 first.
        assertThat(OutboundMessages.class.getDeclaredClasses())
                .extracting(Class::getSimpleName)
                .doesNotContain("OrderConfirmed", "OrderCancelled");
    }
}
