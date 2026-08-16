package com.mohammadanas.saga.scheduler.orchestrator;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins {@link StuckSaga} against saga-orchestrator's {@code StuckSagaResponse}.
 *
 * <p>Same job as order-service's {@code TerminalEventContractTest} and the orchestrator's
 * {@code OutboundContractTest}, for the same reason: the two modules share no code, the
 * wire is JSON resolved against this side's declared record, and a mismatched name arrives
 * as {@code null} rather than as a compile error. Here that would mean a {@code sagaId} of
 * {@code null} being locked and posted back — so the sweep would fail in a way that looks
 * like the orchestrator's fault.
 */
class StuckSagaContractTest {

    private static final ObjectMapper MAPPER =
            JsonMapper.builder().addModule(new JavaTimeModule()).build();

    /** Exactly the fields saga-orchestrator's StuckSagaResponse declares. */
    private static final Set<String> EXPECTED_FIELDS =
            Set.of("sagaId", "orderId", "status", "timeoutDeadline");

    @Test
    @DisplayName("StuckSaga matches the orchestrator's StuckSagaResponse field for field")
    void matchesOrchestratorResponse() throws Exception {
        StuckSaga saga = new StuckSaga(
                UUID.randomUUID(), UUID.randomUUID(), "AWAITING_PAYMENT",
                Instant.parse("2026-08-15T10:00:00Z"));

        Set<String> fields = MAPPER.readTree(MAPPER.writeValueAsString(saga)).properties().stream()
                .map(java.util.Map.Entry::getKey)
                .collect(Collectors.toSet());

        assertThat(fields).containsExactlyInAnyOrderElementsOf(EXPECTED_FIELDS);
    }

    @Test
    @DisplayName("an unrecognised status deserializes rather than blowing up the sweep")
    void unknownStatusIsTolerated() throws Exception {
        // status is a String, not a copy of the orchestrator's enum, precisely so a new
        // saga state added there cannot break this service — which never branches on it.
        String json = """
                {"sagaId":"%s","orderId":"%s","status":"SOME_FUTURE_STATE",
                 "timeoutDeadline":"2026-08-15T10:00:00Z"}
                """.formatted(UUID.randomUUID(), UUID.randomUUID());

        StuckSaga saga = MAPPER.readValue(json, StuckSaga.class);

        assertThat(saga.status()).isEqualTo("SOME_FUTURE_STATE");
        assertThat(saga.sagaId()).isNotNull();
    }
}
