package com.mohammadanas.saga.orchestrator.domain;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface SagaRepository extends JpaRepository<Saga, UUID> {

    Optional<Saga> findByOrderId(UUID orderId);

    /**
     * Sagas the scheduler should sweep: past their deadline and still non-terminal
     * (ARCHITECTURE.md section 4).
     *
     * <p>Both halves of the predicate matter. Dropping the deadline check would sweep
     * every in-flight saga; dropping the status check would re-compensate sagas that
     * already finished, since a terminal saga's deadline stays in the past forever.
     *
     * <p>Terminal statuses are excluded by listing the non-terminal ones explicitly rather
     * than negating, so adding a status later fails loudly here instead of silently
     * enrolling it in the sweep.
     */
    @Query("""
            select s from Saga s
            where s.timeoutDeadline < :now
              and s.status in (
                com.mohammadanas.saga.orchestrator.domain.SagaStatus.STARTED,
                com.mohammadanas.saga.orchestrator.domain.SagaStatus.AWAITING_INVENTORY,
                com.mohammadanas.saga.orchestrator.domain.SagaStatus.AWAITING_PAYMENT,
                com.mohammadanas.saga.orchestrator.domain.SagaStatus.COMPENSATING)
            order by s.timeoutDeadline asc
            """)
    List<Saga> findStuckSagas(@Param("now") Instant now, Limit limit);
}
