package com.mohammadanas.saga.scheduler.lock;

import java.util.Optional;
import java.util.UUID;

/**
 * A distributed mutual exclusion over one saga id (ARCHITECTURE.md section 4).
 *
 * <p>An interface rather than a concrete class so the sweep can be unit-tested without
 * Redis. The behaviour it promises is the whole point of this service being safe to scale:
 * for a given saga id, at most one holder at a time across <em>all</em> scheduler
 * instances.
 *
 * <p>The token returned by {@link #acquire} exists so a release can prove it owns the lock
 * it is releasing. Without it, an instance whose lock had already expired by TTL would
 * happily delete the lock a <em>different</em> instance had since taken, and both would
 * then be inside the critical section believing they were alone. That is the classic way
 * a naive distributed lock fails, and it fails silently.
 */
public interface SagaLock {

    /**
     * Tries to take the lock for {@code sagaId}.
     *
     * @return the ownership token if this caller took the lock, or empty if someone else
     *         holds it. Never blocks and never retries — a saga locked by another instance
     *         is simply skipped this pass and picked up on the next one (§4).
     */
    Optional<String> acquire(UUID sagaId);

    /**
     * Releases the lock, but only if {@code token} still owns it.
     *
     * <p>Safe to call even when the lock has already expired or been taken over; in that
     * case it does nothing rather than stealing it back.
     */
    void release(UUID sagaId, String token);
}
