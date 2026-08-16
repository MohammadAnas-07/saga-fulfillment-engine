package com.mohammadanas.saga.scheduler.sweep;

import com.mohammadanas.saga.scheduler.lock.SagaLock;
import com.mohammadanas.saga.scheduler.orchestrator.OrchestratorClient;
import com.mohammadanas.saga.scheduler.orchestrator.StuckSaga;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * One pass of the timeout sweep (ARCHITECTURE.md section 4): ask what is stuck, and for
 * each one, take its lock before touching it.
 *
 * <p>This class holds no compensation logic and no saga state, by design. It decides
 * <em>when</em> and <em>whether this instance</em>; the orchestrator decides what
 * compensating actually means (§5.1). That split is what keeps the timeout path and the
 * explicit-failure path the same single path.
 */
@Component
public class StuckSagaSweep {

    private static final Logger log = LoggerFactory.getLogger(StuckSagaSweep.class);

    private final OrchestratorClient orchestrator;
    private final SagaLock lock;

    public StuckSagaSweep(OrchestratorClient orchestrator, SagaLock lock) {
        this.orchestrator = orchestrator;
        this.lock = lock;
    }

    /**
     * Runs one pass.
     *
     * <p>The lock is taken <strong>before</strong> the compensation call and released in a
     * {@code finally}, so it is given back whether that call succeeded or threw. Releasing
     * only on success would leave a saga locked until its TTL expired every time the
     * orchestrator hiccupped — and since a failed compensation is exactly the case that
     * needs retrying, that would delay the retry by the full TTL for no reason.
     *
     * <p>A failure on one saga does not abandon the rest of the pass. Each saga is an
     * independent unit of work, and one unreachable moment should not decide the fate of
     * every other stuck saga in the batch.
     */
    public SweepReport sweep() {
        List<StuckSaga> stuck;
        try {
            stuck = orchestrator.findStuckSagas();
        } catch (RuntimeException e) {
            // The orchestrator being unreachable makes the sweep late, not wrong (§4).
            log.error("Could not query stuck sagas; skipping this pass", e);
            return SweepReport.empty();
        }

        if (stuck.isEmpty()) {
            log.debug("Sweep found no stuck sagas");
            return SweepReport.empty();
        }

        log.info("Sweep found {} stuck saga(s)", stuck.size());

        int compensated = 0;
        int skippedLocked = 0;
        int failed = 0;

        for (StuckSaga saga : stuck) {
            Optional<String> token = lock.acquire(saga.sagaId());

            if (token.isEmpty()) {
                // Another instance owns this saga this pass. Expected, not an error.
                skippedLocked++;
                continue;
            }

            try {
                orchestrator.compensate(saga.sagaId());
                compensated++;
            } catch (RuntimeException e) {
                log.error("Compensation call failed for saga {} (stalled in {}); "
                                + "releasing the lock so the next pass can retry",
                        saga.sagaId(), saga.status(), e);
                failed++;
            } finally {
                lock.release(saga.sagaId(), token.get());
            }
        }

        SweepReport report = new SweepReport(stuck.size(), compensated, skippedLocked, failed);
        log.info("Sweep complete: {}", report);
        return report;
    }
}
