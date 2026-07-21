package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.FinalDecision;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

public class MockDecisionEvaluator {
    public static final String RULE_VERSION = "phase1-mock-v1";
    public static final String EVALUATOR_ID = "mock-evaluator";

    public MockEvaluationResult evaluate(UUID applicationId, String caseId, String inputSnapshotVersion) {
        String seed = caseId + ":" + inputSnapshotVersion + ":" + RULE_VERSION;
        UUID evaluationRunId = UUID.nameUUIDFromBytes(("evaluation:" + seed).getBytes(StandardCharsets.UTF_8));
        UUID decisionId = UUID.nameUUIDFromBytes(("decision:" + seed).getBytes(StandardCharsets.UTF_8));
        FinalDecision proposal = inputSnapshotVersion.startsWith("snapshot-v-reject")
                ? FinalDecision.REJECT
                : FinalDecision.APPROVE;
        BigDecimal confidence = proposal == FinalDecision.APPROVE
                ? BigDecimal.valueOf(82, 2)
                : BigDecimal.valueOf(76, 2);
        List<String> reasonCodes = proposal == FinalDecision.APPROVE
                ? List.of("MOCK_RULE_LOW_SYNTHETIC_RISK")
                : List.of("MOCK_RULE_SYNTHETIC_RISK_REVIEW");
        return new MockEvaluationResult(
                evaluationRunId,
                decisionId,
                proposal,
                confidence,
                reasonCodes
        );
    }
}
