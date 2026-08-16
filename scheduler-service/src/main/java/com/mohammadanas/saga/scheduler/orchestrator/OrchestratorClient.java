package com.mohammadanas.saga.scheduler.orchestrator;

import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

/**
 * The scheduler's only conversation with saga-orchestrator: ask what is stuck, then ask for
 * one saga to be compensated.
 *
 * <p>Two calls, and deliberately no third. The scheduler never says <em>how</em> to
 * compensate — it posts a saga id and the orchestrator decides everything else (§4, §5.1).
 * Keeping the interface this narrow is what stops compensation logic drifting into this
 * service, where it would become a second code path to keep in step with the first.
 *
 * <p>Both calls are synchronous REST, the one deliberate exception to §1's no-synchronous-
 * calls rule. The reasoning is in the orchestrator's {@code InternalSagaController}: the
 * scheduler is a liveness mechanism, no saga waits on it, and a failed poll only means the
 * sweep is late.
 */
@Component
public class OrchestratorClient {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorClient.class);

    private static final ParameterizedTypeReference<List<StuckSaga>> STUCK_SAGA_LIST =
            new ParameterizedTypeReference<>() {
            };

    private final RestClient restClient;

    public OrchestratorClient(RestClient orchestratorRestClient) {
        this.restClient = orchestratorRestClient;
    }

    /** Sagas past their deadline and still non-terminal. */
    public List<StuckSaga> findStuckSagas() {
        List<StuckSaga> stuck = restClient.get()
                .uri("/internal/sagas/stuck")
                .retrieve()
                .body(STUCK_SAGA_LIST);

        return stuck == null ? List.of() : stuck;
    }

    /**
     * Asks the orchestrator to compensate one saga.
     *
     * @return the orchestrator's own outcome string, for logging. The scheduler does not
     *         branch on it: every answer — compensated, already compensating, already
     *         terminal, unknown — means the same thing here, which is "nothing further for
     *         me to do with this saga this pass".
     */
    public String compensate(UUID sagaId) {
        String outcome = restClient.post()
                .uri("/internal/sagas/{sagaId}/compensate", sagaId)
                .retrieve()
                .body(String.class);

        log.info("Compensation requested for stuck saga {} -> {}", sagaId, outcome);
        return outcome;
    }
}
