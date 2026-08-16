package com.mohammadanas.saga.scheduler.sweep;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

/**
 * The Quartz job. All it does is call {@link StuckSagaSweep} — the logic lives there so it
 * can be tested without a scheduler running.
 *
 * <p>{@code @DisallowConcurrentExecution} stops one instance from starting a second pass
 * while its previous one is still going, which matters if a sweep ever outruns the
 * interval. It is worth being clear about what this does <strong>not</strong> do: it is a
 * per-scheduler guarantee, so it says nothing about a second scheduler process. Two
 * instances would still sweep simultaneously, and the Redis lock — not this annotation —
 * is what makes that safe (§4).
 */
@DisallowConcurrentExecution
public class StuckSagaSweepJob extends QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(StuckSagaSweepJob.class);

    /**
     * Field injection because Quartz instantiates the job itself, so there is no
     * constructor for Spring to use. Spring Boot's job factory autowires the instance
     * afterwards.
     */
    @Autowired
    private StuckSagaSweep sweep;

    @Override
    protected void executeInternal(JobExecutionContext context) {
        try {
            sweep.sweep();
        } catch (RuntimeException e) {
            // Never let an exception escape into Quartz. An unhandled one would be logged
            // as a job failure and, depending on trigger config, could stop the trigger
            // entirely — turning a transient error into a scheduler that silently stops
            // sweeping. The next pass should simply try again.
            log.error("Sweep pass failed; the next pass will retry", e);
        }
    }
}
