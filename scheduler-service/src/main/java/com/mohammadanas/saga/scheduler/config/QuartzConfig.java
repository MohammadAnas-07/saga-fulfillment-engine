package com.mohammadanas.saga.scheduler.config;

import com.mohammadanas.saga.scheduler.sweep.StuckSagaSweepJob;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.SimpleScheduleBuilder;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Schedules the sweep on a fixed interval (ARCHITECTURE.md section 4).
 *
 * <h2>Why the in-memory job store, and not Quartz clustering</h2>
 *
 * <p>Quartz can coordinate multiple instances itself, via a JDBC job store with clustering
 * enabled, so that only one instance fires a given trigger. That is deliberately not used
 * here.
 *
 * <p>Quartz clustering would coordinate at the wrong granularity: it decides who runs the
 * <em>sweep</em>, which would make every other instance idle and turn the scheduler into an
 * active/passive pair. §4 wants the opposite — every instance sweeping, coordinating per
 * <em>saga</em>, so the work spreads out and one slow instance does not hold up the rest.
 * That is what the Redis lock does, and it is also the mechanism §4 names. Adding Quartz
 * clustering on top would mean two overlapping coordination schemes, a database this
 * service otherwise does not need, and a less honest demonstration of distributed locking.
 *
 * <p>So each instance keeps its own in-memory schedule and fires its own sweep, and the
 * per-saga lock is the only thing arbitrating between them.
 */
@Configuration
public class QuartzConfig {

    @Bean
    public JobDetail stuckSagaSweepJobDetail() {
        return JobBuilder.newJob(StuckSagaSweepJob.class)
                .withIdentity("stuckSagaSweep")
                .withDescription("Compensates sagas past their timeout deadline")
                // Kept in the scheduler across restarts of the trigger; harmless with the
                // in-memory store, and the right default if this ever moves to JDBC.
                .storeDurably()
                .build();
    }

    /**
     * Repeats forever on the configured interval.
     *
     * <p>{@code withMisfireHandlingInstructionNextWithRemainingCount} is the important
     * detail. If the service was down or busy and passes were missed, the default would
     * fire the whole backlog at once — a burst of identical sweeps that would find the same
     * stuck sagas and contend on the same locks. Skipping straight to the next scheduled
     * time is right for a poll: what matters is that a sweep happens soon, not that every
     * historical one is made up.
     */
    @Bean
    public Trigger stuckSagaSweepTrigger(JobDetail stuckSagaSweepJobDetail, SchedulerProperties properties) {
        return TriggerBuilder.newTrigger()
                .forJob(stuckSagaSweepJobDetail)
                .withIdentity("stuckSagaSweepTrigger")
                .withSchedule(SimpleScheduleBuilder.simpleSchedule()
                        .withIntervalInMilliseconds(properties.getInterval().toMillis())
                        .repeatForever()
                        .withMisfireHandlingInstructionNextWithRemainingCount())
                .build();
    }
}
