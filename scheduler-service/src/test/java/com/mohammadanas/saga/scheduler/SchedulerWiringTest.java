package com.mohammadanas.saga.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.scheduler.orchestrator.OrchestratorClient;
import com.mohammadanas.saga.scheduler.orchestrator.StuckSaga;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.TestPropertySource;

/**
 * Proves the Quartz wiring actually schedules and fires the sweep.
 *
 * <p>Worth its own test because everything else here is exercised by calling
 * {@code sweep()} directly. A job that is never triggered — a mis-built trigger, a job
 * class Quartz cannot instantiate, an interval that never elapses — would leave every other
 * test in this module green while the service did nothing at all in production.
 *
 * <p>The orchestrator is mocked; this is about the schedule, not the conversation. Redis is
 * real (and the test skips without it), because the sweep resolves a genuine
 * {@code RedisSagaLock} on its way through.
 */
@SpringBootTest
@TestPropertySource(properties = {
        // Fast enough to observe within the test, and proof the interval is honoured.
        "scheduler.interval=PT1S",
        "scheduler.lock-ttl=PT10S"
})
@EnabledIf(
        value = "redisIsReachable",
        disabledReason = "No Redis on REDIS_HOST:REDIS_PORT (default localhost:6379) — "
                + "start one with: docker run -d --rm -p 6379:6379 redis:7-alpine")
class SchedulerWiringTest {

    @MockBean
    private OrchestratorClient orchestratorClient;

    @Autowired
    private org.quartz.Scheduler quartzScheduler;

    /** Reported as skipped rather than as zero tests — see {@code RedisSagaLockConcurrencyTest}. */
    static boolean redisIsReachable() {
        String host = System.getenv().getOrDefault("REDIS_HOST", "localhost");
        int port = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Test
    @DisplayName("Quartz fires the sweep on the configured interval without anyone calling it")
    void sweepRunsOnItsOwn() {
        when(orchestratorClient.findStuckSagas()).thenReturn(List.of());

        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(250)).untilAsserted(() ->
                verify(orchestratorClient, atLeastOnce()).findStuckSagas());
    }

    @Test
    @DisplayName("a stuck saga reported by the orchestrator gets compensated by the scheduled pass")
    void scheduledPassCompensatesAStuckSaga() {
        UUID sagaId = UUID.randomUUID();
        when(orchestratorClient.findStuckSagas()).thenReturn(List.of(
                new StuckSaga(sagaId, UUID.randomUUID(), "AWAITING_PAYMENT", Instant.now())));
        when(orchestratorClient.compensate(any())).thenReturn("ADVANCED");

        // End to end through the real Quartz trigger and the real Redis lock: nothing in
        // this test calls sweep() itself.
        await().atMost(Duration.ofSeconds(20)).pollInterval(Duration.ofMillis(250)).untilAsserted(() ->
                verify(orchestratorClient, atLeastOnce()).compensate(sagaId));
    }

    @Test
    @DisplayName("the sweep job and its trigger are actually registered with the scheduler")
    void jobAndTriggerAreRegistered() throws Exception {
        assertThat(quartzScheduler.isStarted()).isTrue();
        assertThat(quartzScheduler.checkExists(
                org.quartz.JobKey.jobKey("stuckSagaSweep"))).isTrue();
        assertThat(quartzScheduler.checkExists(
                org.quartz.TriggerKey.triggerKey("stuckSagaSweepTrigger"))).isTrue();
    }
}
