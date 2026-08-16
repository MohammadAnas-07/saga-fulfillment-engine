package com.mohammadanas.saga.scheduler.lock;

import static org.assertj.core.api.Assertions.assertThat;

import com.mohammadanas.saga.scheduler.config.SchedulerProperties;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * <strong>The point of this chunk.</strong> Proves that when several scheduler instances
 * go for the same saga at the same moment, exactly one gets through.
 *
 * <p>Runs against a <em>real</em> Redis, because the guarantee under test is Redis's
 * atomicity and nothing else. A fake or an in-memory stand-in would be testing this test's
 * own map, and would pass just as happily against a broken check-then-set implementation —
 * which is the exact bug that matters here. Simulating the instances with threads rather
 * than processes is fine: they contend through the same Redis, over the same connection
 * pool, with genuinely concurrent commands, and that is where the arbitration happens.
 *
 * <p><strong>Point it at any reachable Redis</strong> via {@code REDIS_HOST} /
 * {@code REDIS_PORT}, defaulting to {@code localhost:6379}. It skips itself if nothing is
 * listening, so the build stays green on a machine without one — but unlike the
 * Testcontainers suites elsewhere in this repo, it needs only a plain Redis on a port,
 * which is why it was possible to actually execute it here:
 *
 * <pre>{@code docker run -d --rm -p 6379:6379 redis:7-alpine}</pre>
 *
 * <p>The skip is deliberately expressed as {@code @EnabledIf} rather than an assumption
 * inside {@code @BeforeAll}. An aborted container reports "0 tests run", which disappears
 * into a green build; a disabled one is counted and named as <em>skipped</em>. Given the
 * README's standing warning that {@code BUILD SUCCESS} does not prove the infrastructure
 * tests ran, this class should not be able to vanish quietly.
 */
@EnabledIf(
        value = "redisIsReachable",
        disabledReason = "No Redis on REDIS_HOST:REDIS_PORT (default localhost:6379) — "
                + "start one with: docker run -d --rm -p 6379:6379 redis:7-alpine")
class RedisSagaLockConcurrencyTest {

    /** Enough contenders that a broken lock is caught reliably, not just occasionally. */
    private static final int CONTENDERS = 16;

    private static final String HOST = System.getenv().getOrDefault("REDIS_HOST", "localhost");
    private static final int PORT = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));

    private static LettuceConnectionFactory connectionFactory;
    private static StringRedisTemplate redis;

    private final List<Runnable> cleanups = new ArrayList<>();

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(HOST, PORT);
        connectionFactory.afterPropertiesSet();

        redis = new StringRedisTemplate(connectionFactory);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void disconnect() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @AfterEach
    void cleanUpKeys() {
        cleanups.forEach(Runnable::run);
        cleanups.clear();
    }

    private static boolean redisIsReachable() {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(HOST, PORT), 1000);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    private SagaLock lockWithTtl(Duration ttl) {
        SchedulerProperties properties = new SchedulerProperties();
        properties.setLockTtl(ttl);
        return new RedisSagaLock(redis, properties);
    }

    private UUID freshSagaId() {
        UUID sagaId = UUID.randomUUID();
        cleanups.add(() -> redis.delete("saga:lock:" + sagaId));
        return sagaId;
    }

    @Test
    @DisplayName("16 instances racing for one saga: exactly one acquires the lock")
    void onlyOneInstanceWinsTheRace() throws Exception {
        UUID sagaId = freshSagaId();
        SagaLock lock = lockWithTtl(Duration.ofMinutes(1));

        // Every thread blocks on the same gate, so they hit Redis together rather than
        // being serialised by their own start-up. Without this the test would pass even
        // against a check-then-set implementation, purely because nothing overlapped.
        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch ready = new CountDownLatch(CONTENDERS);
        AtomicInteger winners = new AtomicInteger();

        ExecutorService pool = Executors.newFixedThreadPool(CONTENDERS);
        try {
            List<Future<Optional<String>>> results = new ArrayList<>();
            for (int i = 0; i < CONTENDERS; i++) {
                results.add(pool.submit((Callable<Optional<String>>) () -> {
                    ready.countDown();
                    startGate.await();
                    Optional<String> token = lock.acquire(sagaId);
                    if (token.isPresent()) {
                        winners.incrementAndGet();
                    }
                    return token;
                }));
            }

            assertThat(ready.await(30, TimeUnit.SECONDS)).as("all contenders ready").isTrue();
            startGate.countDown();

            List<String> tokens = new ArrayList<>();
            for (Future<Optional<String>> result : results) {
                result.get(30, TimeUnit.SECONDS).ifPresent(tokens::add);
            }

            // The assertion the whole chunk exists for.
            assertThat(winners.get()).isEqualTo(1);
            assertThat(tokens).hasSize(1);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    @DisplayName("the loser can acquire once the winner releases — the lock is not a one-shot")
    void lockIsReusableAfterRelease() {
        UUID sagaId = freshSagaId();
        SagaLock lock = lockWithTtl(Duration.ofMinutes(1));

        Optional<String> first = lock.acquire(sagaId);
        assertThat(first).isPresent();
        assertThat(lock.acquire(sagaId)).as("held, so a second attempt fails").isEmpty();

        lock.release(sagaId, first.get());

        Optional<String> second = lock.acquire(sagaId);
        assertThat(second).as("available again once released").isPresent();
        assertThat(second.get()).isNotEqualTo(first.get());
    }

    @Test
    @DisplayName("a release with the wrong token does not free someone else's lock")
    void releaseIsOwnershipChecked() {
        UUID sagaId = freshSagaId();
        SagaLock lock = lockWithTtl(Duration.ofMinutes(1));

        Optional<String> owner = lock.acquire(sagaId);
        assertThat(owner).isPresent();

        // Stands in for an instance whose TTL lapsed and which then tried to tidy up: it
        // must not delete the lock its successor now holds.
        lock.release(sagaId, UUID.randomUUID().toString());

        assertThat(lock.acquire(sagaId))
                .as("still held by the original owner")
                .isEmpty();

        lock.release(sagaId, owner.get());
        assertThat(lock.acquire(sagaId)).as("the real owner can still release it").isPresent();
    }

    @Test
    @DisplayName("an abandoned lock expires by TTL, so a dead instance cannot wedge a saga")
    void lockExpiresByItself() {
        UUID sagaId = freshSagaId();
        SagaLock shortLived = lockWithTtl(Duration.ofSeconds(1));

        assertThat(shortLived.acquire(sagaId)).isPresent();

        // Never released — the instance "died" holding it.
        Long ttl = redis.getExpire("saga:lock:" + sagaId);
        assertThat(ttl).as("a TTL is actually set, not a permanent key").isPositive();

        org.awaitility.Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .pollInterval(Duration.ofMillis(200))
                .untilAsserted(() -> assertThat(shortLived.acquire(sagaId))
                        .as("another instance can take over after expiry")
                        .isPresent());
    }

    @Test
    @DisplayName("locks are per saga: holding one does not block a different saga")
    void locksAreScopedToOneSaga() {
        UUID first = freshSagaId();
        UUID second = freshSagaId();
        SagaLock lock = lockWithTtl(Duration.ofMinutes(1));

        assertThat(lock.acquire(first)).isPresent();

        // If this failed, one stuck saga would serialise the entire sweep across every
        // instance — the coordination would be at the wrong granularity.
        assertThat(lock.acquire(second)).isPresent();
    }

    @Test
    @DisplayName("16 instances across 3 different sagas: exactly one winner per saga")
    void oneWinnerPerSagaAcrossSeveralSagas() throws Exception {
        List<UUID> sagas = List.of(freshSagaId(), freshSagaId(), freshSagaId());
        SagaLock lock = lockWithTtl(Duration.ofMinutes(1));

        CountDownLatch startGate = new CountDownLatch(1);
        CountDownLatch ready = new CountDownLatch(CONTENDERS);
        ExecutorService pool = Executors.newFixedThreadPool(CONTENDERS);

        try {
            List<Future<List<UUID>>> results = new ArrayList<>();
            for (int i = 0; i < CONTENDERS; i++) {
                results.add(pool.submit(() -> {
                    ready.countDown();
                    startGate.await();
                    // Each "instance" sweeps the whole batch, as a real one would.
                    List<UUID> won = new ArrayList<>();
                    for (UUID sagaId : sagas) {
                        lock.acquire(sagaId).ifPresent(token -> won.add(sagaId));
                    }
                    return won;
                }));
            }

            assertThat(ready.await(30, TimeUnit.SECONDS)).isTrue();
            startGate.countDown();

            List<UUID> allWon = new ArrayList<>();
            for (Future<List<UUID>> result : results) {
                allWon.addAll(result.get(30, TimeUnit.SECONDS));
            }

            // Every saga compensated exactly once, and no saga missed.
            assertThat(allWon).containsExactlyInAnyOrderElementsOf(sagas);
        } finally {
            pool.shutdownNow();
        }
    }
}
