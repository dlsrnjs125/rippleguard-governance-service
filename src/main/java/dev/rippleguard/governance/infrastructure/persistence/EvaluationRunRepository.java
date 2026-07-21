package dev.rippleguard.governance.infrastructure.persistence;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface EvaluationRunRepository extends JpaRepository<EvaluationRunEntity, UUID> {
    Optional<EvaluationRunEntity> findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(String caseId);

    Optional<EvaluationRunEntity> findByRequestIdempotencyKey(String requestIdempotencyKey);

    Optional<EvaluationRunEntity> findByAgentRunId(UUID agentRunId);

    @Query(
            """
            select run
            from EvaluationRunEntity run
            where run.status = dev.rippleguard.governance.domain.EvaluationRunStatus.RUNNING
              and (run.nextAttemptAt is null or run.nextAttemptAt <= :now)
              and (run.leaseUntil is null or run.leaseUntil < :now)
            order by run.createdAt
            """
    )
    List<EvaluationRunEntity> findResumablePhase2Runs(@Param("now") Instant now);
}
