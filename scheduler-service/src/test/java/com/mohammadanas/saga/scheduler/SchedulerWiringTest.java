package com.mohammadanas.saga.scheduler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.mohammadanas.saga.scheduler.orchestrator.OrchestratorClient;
import com.mohammadanas.saga.scheduler.orchestrator.StuckSaga;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Proves the Quartz wiring actually schedules and fires the sweep.
 *
 * <p>Worth its own test because everything else here is exercised by calling
 * {@code sweep()} directly. A job that is never triggered — a mis-built trigger, a job
 * class Quartz cannot instantiate, an interval that never elapses — would leave every other
 * test in this module green while the service did nothing at all in production.
 *
 * <p>The orchestrator is mocked; this is about the schedule, not the conversation. Redis is
 * real — in a container since Chunk 8 — because the sweep resolves a genuine
 * {@code RedisSagaLock} on its way through.
 */
@SpringBootTest
@TestPropertySource(properties = {
        // Fast enough to observe within the test, and proof the interval is honoured.
        "scheduler.interval=PT1S",
        "scheduler.lock-ttl=PT10S"
})
@Testcontainers
class SchedulerWiringTest {

    /**
     * Assigned straight from the constructor, then configured — rather than
     * {@code new GenericContainer<>(...).withExposedPorts(...)} as a single expression.
     *
     * <p>A container is {@code AutoCloseable}, and in the chained form the constructor's
     * result is handed to a method before anything holds it, so static analysis cannot see
     * that it is ever owned or closed. It always was — {@code @Testcontainers} stops a
     * {@code @Container} static field after the class finishes — but the warning was
     * pointing at a shape worth not writing, not at a false alarm worth suppressing.
     */
    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine"));

    static {
        REDIS.addExposedPort(6379);
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    /**
     * {@code @MockitoBean}, not {@code @MockBean}: the latter is deprecated in favour of
     * Spring Framework's own bean-override support, which Spring Boot 3.4 adopted. Same
     * behaviour here — the real {@code OrchestratorClient} bean is replaced by a mock for
     * this context — but it is the supported annotation going forward.
     */
    @MockitoBean
    private OrchestratorClient orchestratorClient;

    @Autowired
    private org.quartz.Scheduler quartzScheduler;

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
