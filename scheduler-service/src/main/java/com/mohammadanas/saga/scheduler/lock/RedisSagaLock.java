package com.mohammadanas.saga.scheduler.lock;

import com.mohammadanas.saga.scheduler.config.SchedulerProperties;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * The Redis implementation: {@code SET key token NX PX ttl} to take, compare-and-delete to
 * release.
 *
 * <h2>Why acquisition is one command</h2>
 *
 * <p>{@code setIfAbsent(key, token, ttl)} issues a single {@code SET ... NX PX}. Redis
 * executes it atomically, so of two instances racing for the same saga exactly one gets
 * {@code true}. Checking existence and then writing would be two round trips with a window
 * between them, and both instances would win — which is precisely the bug this class exists
 * to not have, and precisely what the concurrency test asserts.
 *
 * <h2>Why release is a script</h2>
 *
 * <p>A plain {@code DEL} would be wrong. If this instance stalled long enough for its TTL
 * to lapse, another instance may already hold the lock; deleting by key alone would delete
 * <em>theirs</em>, and a third instance could then take it while the second was still
 * working. The Lua script compares the stored token before deleting, and Redis runs the
 * whole script atomically, so a lock is only ever released by its actual owner.
 *
 * <h2>The TTL is the crash backstop</h2>
 *
 * <p>Every acquisition expires on its own. An instance that dies mid-sweep cannot wedge a
 * saga permanently — the lock lapses and the next pass picks it up (§4). That makes the TTL
 * a real tuning parameter rather than a formality: it must comfortably exceed how long
 * compensating one saga takes, or a slow instance loses its lock while still working and
 * two instances end up compensating the same saga. {@link SchedulerProperties} documents
 * the chosen value.
 */
@Component
public class RedisSagaLock implements SagaLock {

    private static final Logger log = LoggerFactory.getLogger(RedisSagaLock.class);

    /** Namespaced so the lock keys are obvious in a shared Redis and easy to scan. */
    private static final String KEY_PREFIX = "saga:lock:";

    /**
     * Delete only if the value still matches our token. Returns 1 if we released it, 0 if
     * it was not ours (expired, or already taken over).
     */
    private static final RedisScript<Long> RELEASE_IF_OWNER = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;
    private final SchedulerProperties properties;

    public RedisSagaLock(StringRedisTemplate redis, SchedulerProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    @Override
    public Optional<String> acquire(UUID sagaId) {
        String token = UUID.randomUUID().toString();
        Boolean acquired = redis.opsForValue()
                .setIfAbsent(key(sagaId), token, properties.getLockTtl());

        if (Boolean.TRUE.equals(acquired)) {
            log.debug("Acquired lock for saga {} (token {})", sagaId, token);
            return Optional.of(token);
        }

        // Entirely normal with more than one instance running: someone else got there
        // first. Skipped this pass, retried next (§4).
        log.debug("Lock for saga {} is held elsewhere; skipping this pass", sagaId);
        return Optional.empty();
    }

    @Override
    public void release(UUID sagaId, String token) {
        Long released = redis.execute(RELEASE_IF_OWNER, List.of(key(sagaId)), token);

        if (released == null || released == 0L) {
            // Means the TTL lapsed while we were working, so the lock may now belong to
            // another instance. Worth a warning: it says the TTL is too short for the work.
            log.warn("Lock for saga {} was not ours to release — TTL likely expired mid-sweep. "
                            + "Consider raising scheduler.lock-ttl.",
                    sagaId);
        }
    }

    private static String key(UUID sagaId) {
        return KEY_PREFIX + sagaId;
    }
}
