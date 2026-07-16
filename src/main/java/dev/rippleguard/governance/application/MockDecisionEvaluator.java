package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.FinalDecision;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Component;

@Component
public class MockDecisionEvaluator {
    public static final String RULE_VERSION = "phase1-mock-v1";
    public static final String EVALUATOR_ID = "mock-evaluator";

    public MockEvaluationResult evaluate(UUID applicationId, String caseId, String inputSnapshotVersion) {
        String seed = applicationId + ":" + caseId + ":" + inputSnapshotVersion + ":" + RULE_VERSION;
        UUID evaluationRunId = UUID.nameUUIDFromBytes(("evaluation:" + seed).getBytes(StandardCharsets.UTF_8));
        UUID decisionId = UUID.nameUUIDFromBytes(("decision:" + seed).getBytes(StandardCharsets.UTF_8));
        int bucket = Math.floorMod(seed.hashCode(), 100);
        FinalDecision proposal = bucket >= 25 ? FinalDecision.APPROVE : FinalDecision.REJECT;
        BigDecimal confidence = BigDecimal.valueOf(70 + (bucket % 25), 2);
        List<String> reasonCodes = proposal == FinalDecision.APPROVE
                ? List.of("MOCK_RULE_LOW_SYNTHETIC_RISK")
                : List.of("MOCK_RULE_SYNTHETIC_RISK_REVIEW");
        return new MockEvaluationResult(
                evaluationRunId,
                decisionId,
                proposal,
                confidence,
                reasonCodes,
                "ASSURANCE_COMPLETE"
        );
    }
}
