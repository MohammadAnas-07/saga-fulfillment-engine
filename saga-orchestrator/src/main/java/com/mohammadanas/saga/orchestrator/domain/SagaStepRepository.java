package com.mohammadanas.saga.orchestrator.domain;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaStepRepository extends JpaRepository<SagaStep, Long> {

    List<SagaStep> findBySagaIdOrderByIdAsc(UUID sagaId);
}
