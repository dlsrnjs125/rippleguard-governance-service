package dev.rippleguard.governance.infrastructure.persistence;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import jakarta.persistence.LockModeType;

public interface DecisionCaseRepository extends JpaRepository<DecisionCaseEntity, String> {
    Optional<DecisionCaseEntity> findByApplicationId(UUID applicationId);

    @Lock(LockModeType.OPTIMISTIC)
    Optional<DecisionCaseEntity> findWithLockByCaseId(String caseId);
}
