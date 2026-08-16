package com.mohammadanas.saga.e2e;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A clock that can be pushed forward, injected into saga-orchestrator in place of its real
 * one.
 *
 * <p>This is what makes the timeout test fast and honest at the same time. A saga's
 * deadline is {@code now(clock) + saga.timeout}, and the stuck-saga query compares against
 * {@code now(clock)} too — so advancing this by ten minutes makes a saga genuinely overdue
 * without the test waiting ten minutes, and without shortening the timeout to something
 * unrealistic that every other test would then have to race against. ARCHITECTURE.md
 * section 8.3 case 3 asks for exactly this: "asserted by advancing a controllable clock,
 * not by sleeping".
 *
 * <p>The offset is deliberately never reset. It applies to saga creation and to the sweep's
 * comparison equally, so a constant shift is invisible to everything except a saga that was
 * already in flight when it moved.
 */
public final class MutableClock extends Clock {

    private final Clock base = Clock.systemUTC();
    private final AtomicReference<Duration> offset = new AtomicReference<>(Duration.ZERO);

    @Override
    public Instant instant() {
        return base.instant().plus(offset.get());
    }

    @Override
    public ZoneId getZone() {
        return base.getZone();
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    /** Pushes every subsequent reading forward. Existing saga deadlines do not move. */
    public void advance(Duration amount) {
        offset.updateAndGet(current -> current.plus(amount));
    }
}
