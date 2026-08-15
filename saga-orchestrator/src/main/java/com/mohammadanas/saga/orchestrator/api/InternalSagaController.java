package com.mohammadanas.saga.orchestrator.api;

import com.mohammadanas.saga.orchestrator.service.SagaOrchestrator;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Internal query API for scheduler-service.
 *
 * <h2>Why REST rather than letting the scheduler read the database</h2>
 *
 * <p>Each service owns its own database (§7), and a service's schema is private. A
 * scheduler reading {@code sagas} directly would couple its deploys to the orchestrator's
 * table layout and let it observe states the orchestrator has not chosen to expose — the
 * table would become an undeclared public API. {@link StuckSagaResponse} is a contract
 * that can stay stable while the entity changes underneath it.
 *
 * <p>§1 rules out synchronous inter-service calls, and this is a deliberate, narrow
 * exception to it. That rule governs the <em>saga workflow</em>: no business step may
 * depend on another service answering a call, because that reintroduces the temporal
 * coupling the saga pattern exists to remove. The scheduler is not a workflow
 * participant — §4 calls it a liveness mechanism, not a correctness one. Nothing in the
 * saga waits on this endpoint, and if it is unavailable the sweep is simply late; no saga
 * becomes incorrect. Modelling a poll as a request/response query is also honest about
 * what it is: asking a question, not announcing a fact.
 *
 * <p>Mapped under {@code /internal} to signal it is not part of the client-facing surface.
 * <strong>It is not actually access-controlled</strong> — authentication is a §1 non-goal
 * — so the prefix is documentation, not a security boundary. A real deployment would keep
 * this off any public listener.
 */
@RestController
@RequestMapping("/internal/sagas")
public class InternalSagaController {

    private final SagaOrchestrator orchestrator;

    public InternalSagaController(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    /** Sagas past their deadline and still non-terminal. Read-only; acting on them is the caller's job. */
    @GetMapping("/stuck")
    public List<StuckSagaResponse> stuckSagas() {
        return orchestrator.findStuckSagas().stream().map(StuckSagaResponse::from).toList();
    }
}
