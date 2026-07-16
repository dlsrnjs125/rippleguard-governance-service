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

    @Column(name = "input_snapshot_version", length = 64)
    private String inputSnapshotVersion;

    @Column(name = "source_payload_hash", nullable = false, length = 64)
    private String sourcePayloadHash;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 64)
    private DecisionCaseStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "final_decision", length = 32)
    private FinalDecision finalDecision;

    @Column(name = "assurance_result", length = 64)
    private String assuranceResult;

    @Column(name = "reason_code", length = 128)
    private String reasonCode;

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
                              String inputSnapshotVersion, String sourcePayloadHash, Instant now) {
        this.caseId = caseId;
        this.applicationId = applicationId;
        this.applicantId = applicantId;
        this.inputSnapshotVersion = inputSnapshotVersion;
        this.sourcePayloadHash = sourcePayloadHash;
        this.status = DecisionCaseStatus.CREATED;
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
        transitionTo(DecisionCaseStatus.PROPOSAL_READY, now);
    }

    public void commandDecision(FinalDecision finalDecision, String assuranceResult, Instant now) {
        transitionTo(DecisionCaseStatus.ASSURANCE_EVALUATED, now);
        transitionTo(DecisionCaseStatus.RESOLVED, now);
        this.finalDecision = finalDecision;
        this.assuranceResult = assuranceResult;
    }

    public void markPreflightCompleted(Instant now) {
        transitionTo(DecisionCaseStatus.PREFLIGHT_COMPLETED, now);
    }

    public void markVerificationRequired(String reasonCode, String assuranceResult, Instant now) {
        transitionTo(DecisionCaseStatus.VERIFICATION_REQUIRED, now);
        this.reasonCode = reasonCode;
        this.assuranceResult = assuranceResult;
    }

    public void markBlocked(String reasonCode, String assuranceResult, Instant now) {
        transitionTo(DecisionCaseStatus.BLOCKED, now);
        this.reasonCode = reasonCode;
        this.assuranceResult = assuranceResult;
    }

    public void markRecalculationRequired(String reasonCode, Instant now) {
        transitionTo(DecisionCaseStatus.RECALCULATION_REQUIRED, now);
        this.reasonCode = reasonCode;
    }

    private boolean canTransitionTo(DecisionCaseStatus target) {
        return switch (status) {
            case CREATED -> target == DecisionCaseStatus.PREFLIGHT_COMPLETED
                    || target == DecisionCaseStatus.VERIFICATION_REQUIRED
                    || target == DecisionCaseStatus.BLOCKED
                    || target == DecisionCaseStatus.RECALCULATION_REQUIRED;
            case PREFLIGHT_COMPLETED -> target == DecisionCaseStatus.EVALUATION_REQUESTED
                    || target == DecisionCaseStatus.VERIFICATION_REQUIRED
                    || target == DecisionCaseStatus.BLOCKED
                    || target == DecisionCaseStatus.RECALCULATION_REQUIRED;
            case EVALUATION_REQUESTED -> target == DecisionCaseStatus.PROPOSAL_READY
                    || target == DecisionCaseStatus.VERIFICATION_REQUIRED
                    || target == DecisionCaseStatus.BLOCKED
                    || target == DecisionCaseStatus.RECALCULATION_REQUIRED;
            case PROPOSAL_READY -> target == DecisionCaseStatus.ASSURANCE_EVALUATED
                    || target == DecisionCaseStatus.VERIFICATION_REQUIRED
                    || target == DecisionCaseStatus.BLOCKED
                    || target == DecisionCaseStatus.RECALCULATION_REQUIRED;
            case ASSURANCE_EVALUATED -> target == DecisionCaseStatus.RESOLVED
                    || target == DecisionCaseStatus.BLOCKED
                    || target == DecisionCaseStatus.RECALCULATION_REQUIRED;
            case VERIFICATION_REQUIRED, BLOCKED, RESOLVED, RECALCULATION_REQUIRED ->
                    target == DecisionCaseStatus.RECALCULATION_REQUIRED;
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

    public String getSourcePayloadHash() {
        return sourcePayloadHash;
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

    public String getReasonCode() {
        return reasonCode;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }
}
