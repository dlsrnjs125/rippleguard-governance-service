package dev.rippleguard.governance.infrastructure.persistence;

import dev.rippleguard.governance.domain.EvaluationRunStatus;
import dev.rippleguard.governance.domain.FinalDecision;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "evaluation_run")
public class EvaluationRunEntity {
    @Id
    @Column(name = "evaluation_run_id")
    private UUID evaluationRunId;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "case_id", nullable = false)
    private DecisionCaseEntity decisionCase;

    @Column(name = "rule_version", nullable = false, length = 64)
    private String ruleVersion;

    @Column(name = "execution_plan_version", nullable = false, length = 64)
    private String executionPlanVersion;

    @Column(name = "component_versions", nullable = false, columnDefinition = "text")
    private String componentVersions;

    @Column(name = "policy_input_version", nullable = false, length = 64)
    private String policyInputVersion;

    @Column(name = "policy_bundle_version", nullable = false, length = 64)
    private String policyBundleVersion;

    @Column(name = "supersedes_run_id")
    private UUID supersedesRunId;

    @Column(name = "input_snapshot_version", nullable = false, length = 64)
    private String inputSnapshotVersion;

    @Column(name = "decision_id", nullable = false, unique = true)
    private UUID decisionId;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 32)
    private EvaluationRunStatus status;

    @Enumerated(EnumType.STRING)
    @Column(name = "proposal", length = 32)
    private FinalDecision proposal;

    @Column(name = "confidence", precision = 5, scale = 4)
    private BigDecimal confidence;

    @Column(name = "reason_codes", columnDefinition = "text")
    private String reasonCodes;

    @Column(name = "agent_run_id")
    private UUID agentRunId;

    @Column(name = "request_idempotency_key", length = 160)
    private String requestIdempotencyKey;

    @Column(name = "snapshot_id", length = 128)
    private String snapshotId;

    @Column(name = "snapshot_schema_version", length = 64)
    private String snapshotSchemaVersion;

    @Column(name = "snapshot_digest", length = 80)
    private String snapshotDigest;

    @Column(name = "feature_schema_version", length = 128)
    private String featureSchemaVersion;

    @Column(name = "preprocessing_version", length = 128)
    private String preprocessingVersion;

    @Column(name = "model_version", length = 128)
    private String modelVersion;

    @Column(name = "model_artifact_digest", length = 80)
    private String modelArtifactDigest;

    @Column(name = "threshold_version", length = 128)
    private String thresholdVersion;

    @Column(name = "attempt_count", nullable = false)
    private int attemptCount;

    @Column(name = "max_attempts", nullable = false)
    private int maxAttempts = 1;

    @Column(name = "requested_at")
    private Instant requestedAt;

    @Column(name = "deadline_at")
    private Instant deadlineAt;

    @Column(name = "failure_classification", length = 64)
    private String failureClassification;

    @Column(name = "failure_reason_code", length = 128)
    private String failureReasonCode;

    @Column(name = "accepted_result_digest", length = 80)
    private String acceptedResultDigest;

    @Column(name = "accepted_proposal_snapshot", columnDefinition = "text")
    private String acceptedProposalSnapshot;

    @Column(name = "validation_outcome", length = 32)
    private String validationOutcome;

    @Column(name = "source_event_id")
    private UUID sourceEventId;

    @Column(name = "snapshot_created_at")
    private Instant snapshotCreatedAt;

    @Column(name = "snapshot_reference", columnDefinition = "text")
    private String snapshotReference;

    @Column(name = "reference_type", length = 64)
    private String referenceType;

    @Column(name = "last_attempt_started_at")
    private Instant lastAttemptStartedAt;

    @Column(name = "next_attempt_at")
    private Instant nextAttemptAt;

    @Column(name = "lease_owner", length = 128)
    private String leaseOwner;

    @Column(name = "lease_until")
    private Instant leaseUntil;

    @Column(name = "last_transport_failure_code", length = 128)
    private String lastTransportFailureCode;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Version
    @Column(nullable = false)
    private long version;

    protected EvaluationRunEntity() {
    }

    public EvaluationRunEntity(UUID evaluationRunId, DecisionCaseEntity decisionCase, String ruleVersion,
                               String inputSnapshotVersion, UUID decisionId, String componentVersions,
                               Instant createdAt) {
        this(evaluationRunId, decisionCase, ruleVersion, inputSnapshotVersion, decisionId, componentVersions, null, createdAt);
    }

    public EvaluationRunEntity(UUID evaluationRunId, DecisionCaseEntity decisionCase, String ruleVersion,
                               String inputSnapshotVersion, UUID decisionId, String componentVersions,
                               UUID supersedesRunId, Instant createdAt) {
        this.evaluationRunId = evaluationRunId;
        this.decisionCase = decisionCase;
        this.ruleVersion = ruleVersion;
        this.executionPlanVersion = ruleVersion;
        this.componentVersions = componentVersions;
        this.policyInputVersion = "phase1-no-opa";
        this.policyBundleVersion = "phase1-no-opa";
        this.supersedesRunId = supersedesRunId;
        this.inputSnapshotVersion = inputSnapshotVersion;
        this.decisionId = decisionId;
        this.status = EvaluationRunStatus.CREATED;
        this.createdAt = createdAt;
    }

    public void configurePhase2(String executionPlanVersion,
                                UUID agentRunId,
                                String requestIdempotencyKey,
                                String snapshotId,
                                String snapshotSchemaVersion,
                                String snapshotDigest,
                                UUID sourceEventId,
                                Instant snapshotCreatedAt,
                                String snapshotReference,
                                String referenceType,
                                String featureSchemaVersion,
                                String preprocessingVersion,
                                String modelVersion,
                                String modelArtifactDigest,
                                String thresholdVersion,
                                int maxAttempts,
                                Instant requestedAt,
                                Instant deadlineAt) {
        if (status != EvaluationRunStatus.CREATED) {
            throw new IllegalStateException("Evaluation run cannot be configured after start: " + status);
        }
        this.executionPlanVersion = executionPlanVersion;
        this.ruleVersion = executionPlanVersion;
        this.agentRunId = agentRunId;
        this.requestIdempotencyKey = requestIdempotencyKey;
        this.snapshotId = snapshotId;
        this.snapshotSchemaVersion = snapshotSchemaVersion;
        this.snapshotDigest = snapshotDigest;
        this.sourceEventId = sourceEventId;
        this.snapshotCreatedAt = snapshotCreatedAt;
        this.snapshotReference = snapshotReference;
        this.referenceType = referenceType;
        this.featureSchemaVersion = featureSchemaVersion;
        this.preprocessingVersion = preprocessingVersion;
        this.modelVersion = modelVersion;
        this.modelArtifactDigest = modelArtifactDigest;
        this.thresholdVersion = thresholdVersion;
        this.maxAttempts = maxAttempts;
        this.requestedAt = requestedAt;
        this.deadlineAt = deadlineAt;
        this.nextAttemptAt = requestedAt;
    }

    public void start() {
        if (status != EvaluationRunStatus.CREATED) {
            throw new IllegalStateException("Evaluation run is not startable: " + status);
        }
        this.status = EvaluationRunStatus.RUNNING;
    }

    public void complete(FinalDecision proposal, BigDecimal confidence, String reasonCodes, Instant completedAt) {
        if (status != EvaluationRunStatus.RUNNING) {
            throw new IllegalStateException("Evaluation run is not running: " + status);
        }
        this.status = EvaluationRunStatus.COMPLETED;
        this.proposal = proposal;
        this.confidence = confidence;
        this.reasonCodes = reasonCodes;
        this.completedAt = completedAt;
    }

    public void recordAttempt(int attemptId) {
        this.attemptCount = Math.max(this.attemptCount, attemptId);
    }

    public void recordAttemptStarted(int attemptId, Instant startedAt, String leaseOwner, Instant leaseUntil) {
        if (status != EvaluationRunStatus.RUNNING) {
            throw new IllegalStateException("Evaluation run is not running: " + status);
        }
        recordAttempt(attemptId);
        this.lastAttemptStartedAt = startedAt;
        this.leaseOwner = leaseOwner;
        this.leaseUntil = leaseUntil;
        this.nextAttemptAt = startedAt;
        this.lastTransportFailureCode = null;
    }

    public void recordRetryableTransportFailure(String reasonCode, Instant now, Instant nextAttemptAt) {
        if (status != EvaluationRunStatus.RUNNING) {
            throw new IllegalStateException("Evaluation run is not running: " + status);
        }
        this.failureClassification = "RETRYABLE";
        this.failureReasonCode = reasonCode;
        this.lastTransportFailureCode = reasonCode;
        this.nextAttemptAt = nextAttemptAt;
        this.leaseOwner = null;
        this.leaseUntil = null;
    }

    public void releaseExecutionLease() {
        this.leaseOwner = null;
        this.leaseUntil = null;
    }

    public void failOrchestration(String classification, String reasonCode, Instant completedAt) {
        if (status != EvaluationRunStatus.RUNNING) {
            throw new IllegalStateException("Evaluation run is not running: " + status);
        }
        this.status = EvaluationRunStatus.FAILED;
        this.failureClassification = classification;
        this.failureReasonCode = reasonCode;
        this.validationOutcome = "REJECTED";
        this.completedAt = completedAt;
        this.leaseOwner = null;
        this.leaseUntil = null;
    }

    public void completePhase2(String acceptedResultDigest,
                               String acceptedProposalSnapshot,
                               String reasonCodes,
                               Instant completedAt) {
        if (status != EvaluationRunStatus.RUNNING) {
            throw new IllegalStateException("Evaluation run is not running: " + status);
        }
        this.status = EvaluationRunStatus.COMPLETED;
        this.acceptedResultDigest = acceptedResultDigest;
        this.acceptedProposalSnapshot = acceptedProposalSnapshot;
        this.reasonCodes = reasonCodes;
        this.validationOutcome = "VALIDATED";
        this.completedAt = completedAt;
        this.leaseOwner = null;
        this.leaseUntil = null;
    }

    public void rejectPhase2(String classification, String reasonCode, String acceptedResultDigest, Instant completedAt) {
        if (status != EvaluationRunStatus.RUNNING) {
            throw new IllegalStateException("Evaluation run is not running: " + status);
        }
        this.status = "BLOCKED".equals(classification) ? EvaluationRunStatus.BLOCKED : EvaluationRunStatus.FAILED;
        this.failureClassification = classification;
        this.failureReasonCode = reasonCode;
        this.acceptedResultDigest = acceptedResultDigest;
        this.validationOutcome = "REJECTED";
        this.completedAt = completedAt;
        this.leaseOwner = null;
        this.leaseUntil = null;
    }

    public UUID getEvaluationRunId() {
        return evaluationRunId;
    }

    public DecisionCaseEntity getDecisionCase() {
        return decisionCase;
    }

    public String getRuleVersion() {
        return ruleVersion;
    }

    public String getExecutionPlanVersion() {
        return executionPlanVersion;
    }

    public String getComponentVersions() {
        return componentVersions;
    }

    public String getPolicyInputVersion() {
        return policyInputVersion;
    }

    public String getPolicyBundleVersion() {
        return policyBundleVersion;
    }

    public UUID getSupersedesRunId() {
        return supersedesRunId;
    }

    public String getInputSnapshotVersion() {
        return inputSnapshotVersion;
    }

    public UUID getDecisionId() {
        return decisionId;
    }

    public EvaluationRunStatus getStatus() {
        return status;
    }

    public FinalDecision getProposal() {
        return proposal;
    }

    public BigDecimal getConfidence() {
        return confidence;
    }

    public String getReasonCodes() {
        return reasonCodes;
    }

    public UUID getAgentRunId() {
        return agentRunId;
    }

    public String getRequestIdempotencyKey() {
        return requestIdempotencyKey;
    }

    public String getSnapshotId() {
        return snapshotId;
    }

    public String getSnapshotSchemaVersion() {
        return snapshotSchemaVersion;
    }

    public String getSnapshotDigest() {
        return snapshotDigest;
    }

    public String getFeatureSchemaVersion() {
        return featureSchemaVersion;
    }

    public String getPreprocessingVersion() {
        return preprocessingVersion;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public String getModelArtifactDigest() {
        return modelArtifactDigest;
    }

    public String getThresholdVersion() {
        return thresholdVersion;
    }

    public int getAttemptCount() {
        return attemptCount;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public Instant getRequestedAt() {
        return requestedAt;
    }

    public Instant getDeadlineAt() {
        return deadlineAt;
    }

    public String getFailureClassification() {
        return failureClassification;
    }

    public String getFailureReasonCode() {
        return failureReasonCode;
    }

    public String getAcceptedResultDigest() {
        return acceptedResultDigest;
    }

    public String getAcceptedProposalSnapshot() {
        return acceptedProposalSnapshot;
    }

    public String getValidationOutcome() {
        return validationOutcome;
    }

    public UUID getSourceEventId() {
        return sourceEventId;
    }

    public Instant getSnapshotCreatedAt() {
        return snapshotCreatedAt;
    }

    public String getSnapshotReference() {
        return snapshotReference;
    }

    public String getReferenceType() {
        return referenceType;
    }

    public Instant getLastAttemptStartedAt() {
        return lastAttemptStartedAt;
    }

    public Instant getNextAttemptAt() {
        return nextAttemptAt;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public Instant getLeaseUntil() {
        return leaseUntil;
    }

    public String getLastTransportFailureCode() {
        return lastTransportFailureCode;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }
}
