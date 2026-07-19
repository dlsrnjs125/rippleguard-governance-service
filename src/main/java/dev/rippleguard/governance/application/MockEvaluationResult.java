package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.FinalDecision;
import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;

public record MockEvaluationResult(
        UUID evaluationRunId,
        UUID decisionId,
        FinalDecision proposal,
        BigDecimal confidence,
        List<String> reasonCodes
) {
}
