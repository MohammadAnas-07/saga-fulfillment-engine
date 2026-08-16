package com.mohammadanas.saga.scheduler.sweep;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.scheduler.lock.SagaLock;
import com.mohammadanas.saga.scheduler.orchestrator.OrchestratorClient;
import com.mohammadanas.saga.scheduler.orchestrator.StuckSaga;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * The sweep's decision-making, with the lock and the orchestrator mocked out.
 *
 * <p>That the lock genuinely excludes concurrent instances is proved against real Redis in
 * {@code RedisSagaLockConcurrencyTest} — that is the guarantee, and a mock cannot testify
 * to it. What these tests cover is the part that is this class's own responsibility: that
 * the lock is taken <em>before</em> acting, that it is always given back, and that one bad
 * saga does not take the pass down with it.
 */
@ExtendWith(MockitoExtension.class)
class StuckSagaSweepTest {

    private static final Instant DEADLINE = Instant.parse("2026-08-15T10:00:00Z");

    @Mock
    private OrchestratorClient orchestrator;

    @Mock
    private SagaLock lock;

    private StuckSagaSweep sweep;

    private static StuckSaga stuck(UUID sagaId) {
        return new StuckSaga(sagaId, UUID.randomUUID(), "AWAITING_PAYMENT", DEADLINE);
    }

    private StuckSagaSweep sweepUnderTest() {
        if (sweep == null) {
            sweep = new StuckSagaSweep(orchestrator, lock);
        }
        return sweep;
    }

    @Nested
    @DisplayName("locking comes first")
    class LockOrdering {

        @Test
        @DisplayName("the lock is acquired before the compensation call, never after")
        void locksBeforeActing() {
            UUID sagaId = UUID.randomUUID();
            when(orchestrator.findStuckSagas()).thenReturn(List.of(stuck(sagaId)));
            when(lock.acquire(sagaId)).thenReturn(Optional.of("token"));

            sweepUnderTest().sweep();

            // Ordering is the whole guarantee. Compensating first and locking afterwards
            // would let two instances both compensate and then politely take turns
            // recording that they had.
            InOrder inOrder = Mockito.inOrder(lock, orchestrator);
            inOrder.verify(lock).acquire(sagaId);
            inOrder.verify(orchestrator).compensate(sagaId);
            inOrder.verify(lock).release(sagaId, "token");
        }

        @Test
        @DisplayName("a saga locked by another instance is skipped, not compensated")
        void skipsSagaLockedElsewhere() {
            UUID sagaId = UUID.randomUUID();
            when(orchestrator.findStuckSagas()).thenReturn(List.of(stuck(sagaId)));
            when(lock.acquire(sagaId)).thenReturn(Optional.empty());

            SweepReport report = sweepUnderTest().sweep();

            verify(orchestrator, never()).compensate(any());
            // Nothing was taken, so nothing may be given back — releasing a lock we never
            // held would free the instance that does hold it.
            verify(lock, never()).release(any(), any());
            assertThat(report.skippedLocked()).isEqualTo(1);
            assertThat(report.compensated()).isZero();
        }

        @Test
        @DisplayName("each saga is locked individually, so one held saga does not block the rest")
        void locksPerSaga() {
            UUID held = UUID.randomUUID();
            UUID free = UUID.randomUUID();
            when(orchestrator.findStuckSagas()).thenReturn(List.of(stuck(held), stuck(free)));
            when(lock.acquire(held)).thenReturn(Optional.empty());
            when(lock.acquire(free)).thenReturn(Optional.of("token"));

            SweepReport report = sweepUnderTest().sweep();

            verify(orchestrator).compensate(free);
            verify(orchestrator, never()).compensate(held);
            assertThat(report.compensated()).isEqualTo(1);
            assertThat(report.skippedLocked()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("the lock is always given back")
    class ReleaseIsGuaranteed {

        @Test
        @DisplayName("released after a successful compensation")
        void releasesOnSuccess() {
            UUID sagaId = UUID.randomUUID();
            when(orchestrator.findStuckSagas()).thenReturn(List.of(stuck(sagaId)));
            when(lock.acquire(sagaId)).thenReturn(Optional.of("token"));

            sweepUnderTest().sweep();

            verify(lock).release(sagaId, "token");
        }

        @Test
        @DisplayName("released when the compensation call throws — not only on success")
        void releasesOnFailure() {
            UUID sagaId = UUID.randomUUID();
            when(orchestrator.findStuckSagas()).thenReturn(List.of(stuck(sagaId)));
            when(lock.acquire(sagaId)).thenReturn(Optional.of("token"));
            when(orchestrator.compensate(sagaId)).thenThrow(new RuntimeException("orchestrator down"));

            SweepReport report = sweepUnderTest().sweep();

            // Holding the lock until the TTL lapsed would delay the retry of the very saga
            // that just failed — the one case that most needs retrying promptly.
            verify(lock).release(sagaId, "token");
            assertThat(report.failed()).isEqualTo(1);
            assertThat(report.compensated()).isZero();
        }

        @Test
        @DisplayName("one failing saga does not abandon the others in the batch")
        void oneFailureDoesNotAbortThePass() {
            UUID failing = UUID.randomUUID();
            UUID healthy = UUID.randomUUID();
            when(orchestrator.findStuckSagas()).thenReturn(List.of(stuck(failing), stuck(healthy)));
            when(lock.acquire(failing)).thenReturn(Optional.of("token-1"));
            when(lock.acquire(healthy)).thenReturn(Optional.of("token-2"));
            when(orchestrator.compensate(failing)).thenThrow(new RuntimeException("boom"));

            SweepReport report = sweepUnderTest().sweep();

            verify(orchestrator).compensate(healthy);
            verify(lock).release(failing, "token-1");
            verify(lock).release(healthy, "token-2");
            assertThat(report.compensated()).isEqualTo(1);
            assertThat(report.failed()).isEqualTo(1);
        }
    }

    @Nested
    @DisplayName("degrading rather than failing")
    class Degradation {

        @Test
        @DisplayName("an unreachable orchestrator makes the sweep late, not wrong")
        void queryFailureIsSurvivable() {
            when(orchestrator.findStuckSagas()).thenThrow(new RuntimeException("connection refused"));

            SweepReport report = sweepUnderTest().sweep();

            // No exception escapes, no lock is touched, and the next pass simply tries
            // again — the scheduler is a liveness mechanism, not a correctness one (§4).
            assertThat(report).isEqualTo(SweepReport.empty());
            Mockito.verifyNoInteractions(lock);
        }

        @Test
        @DisplayName("nothing stuck is the ordinary case and touches no locks")
        void noStuckSagasIsANoOp() {
            when(orchestrator.findStuckSagas()).thenReturn(List.of());

            assertThat(sweepUnderTest().sweep()).isEqualTo(SweepReport.empty());
            Mockito.verifyNoInteractions(lock);
        }
    }
}
