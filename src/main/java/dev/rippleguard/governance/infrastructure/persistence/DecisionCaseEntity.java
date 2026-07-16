package dev.rippleguard.governance.infrastructure.persistence;

import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.FinalDecision;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "decision_case")
public class DecisionCaseEntity {
    @Id
    @Column(name = "case_id", length = 128)
    private String caseId;

    @Column(name = "application_id", nullable = false, unique = true)
    private UUID applicationId;

    @Column(name = "applicant_id", nullable = false)
    private String applicantId;

    @Column(name = "input_snapshot_version", nullable = false, length = 64)
    private String inputSnapshotVersion;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 64)
    private DecisionCaseStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "final_decision", length = 32)
    private FinalDecision finalDecision;

    @Column(name = "assurance_result", length = 64)
    private String assuranceResult;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @Version
    @Column(nullable = false)
    private long version;

    protected DecisionCaseEntity() {
    }

    public DecisionCaseEntity(String caseId, UUID applicationId, String applicantId,
                              String inputSnapshotVersion, Instant now) {
        this.caseId = caseId;
        this.applicationId = applicationId;
        this.applicantId = applicantId;
        this.inputSnapshotVersion = inputSnapshotVersion;
        this.status = DecisionCaseStatus.REVIEW_STARTED;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void transitionTo(DecisionCaseStatus target, Instant now) {
        if (!canTransitionTo(target)) {
            throw new IllegalStateException("Transition denied: " + status + " -> " + target);
        }
        status = target;
        updatedAt = now;
    }

    public void completeEvaluation(Instant now) {
        transitionTo(DecisionCaseStatus.EVALUATION_COMPLETED, now);
    }

    public void commandDecision(FinalDecision finalDecision, String assuranceResult, Instant now) {
        transitionTo(DecisionCaseStatus.DECISION_COMMANDED, now);
        this.finalDecision = finalDecision;
        this.assuranceResult = assuranceResult;
    }

    private boolean canTransitionTo(DecisionCaseStatus target) {
        return switch (status) {
            case REVIEW_STARTED -> target == DecisionCaseStatus.EVALUATION_REQUESTED || target == DecisionCaseStatus.FAILED;
            case EVALUATION_REQUESTED -> target == DecisionCaseStatus.EVALUATION_COMPLETED || target == DecisionCaseStatus.FAILED;
            case EVALUATION_COMPLETED -> target == DecisionCaseStatus.DECISION_COMMANDED || target == DecisionCaseStatus.FAILED;
            case DECISION_COMMANDED, FAILED -> false;
        };
    }

    public String getCaseId() {
        return caseId;
    }

    public UUID getApplicationId() {
        return applicationId;
    }

    public String getApplicantId() {
        return applicantId;
    }

    public String getInputSnapshotVersion() {
        return inputSnapshotVersion;
    }

    public DecisionCaseStatus getStatus() {
        return status;
    }

    public FinalDecision getFinalDecision() {
        return finalDecision;
    }

    public String getAssuranceResult() {
        return assuranceResult;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
