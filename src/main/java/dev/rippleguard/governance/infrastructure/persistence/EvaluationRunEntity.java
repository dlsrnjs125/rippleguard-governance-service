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
        this.evaluationRunId = evaluationRunId;
        this.decisionCase = decisionCase;
        this.ruleVersion = ruleVersion;
        this.executionPlanVersion = ruleVersion;
        this.componentVersions = componentVersions;
        this.policyInputVersion = "phase1-no-opa";
        this.policyBundleVersion = "phase1-no-opa";
        this.supersedesRunId = null;
        this.inputSnapshotVersion = inputSnapshotVersion;
        this.decisionId = decisionId;
        this.status = EvaluationRunStatus.CREATED;
        this.createdAt = createdAt;
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

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }
}
