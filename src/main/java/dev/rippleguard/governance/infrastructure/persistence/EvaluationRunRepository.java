package dev.rippleguard.governance.infrastructure.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface EvaluationRunRepository extends JpaRepository<EvaluationRunEntity, UUID> {
    Optional<EvaluationRunEntity> findFirstByDecisionCaseCaseIdOrderByRequestedAtDesc(String caseId);
}
