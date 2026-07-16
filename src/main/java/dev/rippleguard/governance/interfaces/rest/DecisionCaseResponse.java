package dev.rippleguard.governance.interfaces.rest;

import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.EvaluationRunStatus;
import dev.rippleguard.governance.domain.FinalDecision;
import java.time.Instant;
import java.util.UUID;

public record DecisionCaseResponse(
        String schemaVersion,
        String caseId,
        UUID applicationId,
        DecisionCaseStatus status,
        String inputSnapshotVersion,
        UUID evaluationRunId,
        EvaluationRunStatus evaluationRunStatus,
        FinalDecision proposal,
        FinalDecision finalDecision,
        String assuranceResult,
        Instant createdAt,
        Instant updatedAt
) {
}
