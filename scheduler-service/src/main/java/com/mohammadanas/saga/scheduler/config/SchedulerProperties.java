package com.mohammadanas.saga.scheduler.config;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Scheduler tuning.
 *
 * <h2>interval</h2>
 *
 * <p>How often the sweep runs. 30 seconds by default: frequent enough that a stuck saga is
 * resolved promptly, rare enough that the orchestrator is not being polled constantly for
 * what is almost always an empty list. The sweep is a liveness mechanism, not a correctness
 * one (§4) — nothing becomes wrong because a pass happened a little later.
 *
 * <h2>lockTtl</h2>
 *
 * <p>How long a saga's lock survives without being released. This is a real safety
 * parameter, not a formality, and it is bounded from both sides:
 *
 * <ul>
 *   <li><strong>Too short</strong> and a slow instance loses its lock while still working;
 *       a second instance then compensates the same saga concurrently, which is the exact
 *       thing the lock exists to prevent.
 *   <li><strong>Too long</strong> and a saga whose holder crashed stays untouchable for
 *       that whole period. Since the holder is only making one REST call, that is wasted
 *       time rather than damage.
 * </ul>
 *
 * <p>Two minutes is deliberately generous against a single {@code POST} to the
 * orchestrator: it errs toward the harmless failure. §4 states the constraint as "the lock
 * TTL must exceed the expected handling time".
 *
 * <h2>orchestratorBaseUrl</h2>
 *
 * <p>Where to poll. There is no service discovery in this project and none planned (§1
 * non-goals), so this is plain configuration.
 */
@ConfigurationProperties(prefix = "scheduler")
public class SchedulerProperties {

    private Duration interval = Duration.ofSeconds(30);

    private Duration lockTtl = Duration.ofMinutes(2);

    private String orchestratorBaseUrl = "http://localhost:8085";

    /** How long to wait on the orchestrator before giving up and letting the next pass retry. */
    private Duration requestTimeout = Duration.ofSeconds(10);

    public Duration getInterval() {
        return interval;
    }

    public void setInterval(Duration interval) {
        this.interval = interval;
    }

    public Duration getLockTtl() {
        return lockTtl;
    }

    public void setLockTtl(Duration lockTtl) {
        this.lockTtl = lockTtl;
    }

    public String getOrchestratorBaseUrl() {
        return orchestratorBaseUrl;
    }

    public void setOrchestratorBaseUrl(String orchestratorBaseUrl) {
        this.orchestratorBaseUrl = orchestratorBaseUrl;
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = requestTimeout;
    }
}
