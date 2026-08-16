package com.mohammadanas.saga.scheduler.sweep;

/**
 * What one pass did. Returned rather than only logged so the behaviour is assertable in
 * tests without reading log output.
 *
 * @param found       stuck sagas the orchestrator reported
 * @param compensated sagas this instance locked and successfully asked to compensate
 * @param skippedLocked sagas another instance held the lock for. Not a failure — the
 *                      expected outcome when more than one instance is running
 * @param failed      sagas this instance locked but whose compensation call errored. The
 *                    lock is still released; the next pass retries
 */
public record SweepReport(int found, int compensated, int skippedLocked, int failed) {

    public static SweepReport empty() {
        return new SweepReport(0, 0, 0, 0);
    }
}
