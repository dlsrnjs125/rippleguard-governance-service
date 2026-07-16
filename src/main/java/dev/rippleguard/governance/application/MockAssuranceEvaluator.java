package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.AssuranceResult;
import org.springframework.stereotype.Component;

@Component
public class MockAssuranceEvaluator {
    public MockAssuranceResult evaluate(LoanApplicationSubmittedPayload payload, MockEvaluationResult proposal) {
        if (payload.inputSnapshotVersion() == null || payload.inputSnapshotVersion().isBlank()) {
            return new MockAssuranceResult(AssuranceResult.ASSURANCE_INCOMPLETE, "SNAPSHOT_REFERENCE_MISSING");
        }
        if (payload.inputSnapshotVersion().startsWith("snapshot-v-blocked")) {
            return new MockAssuranceResult(AssuranceResult.ASSURANCE_VIOLATED, "UNSUPPORTED_MOCK_SNAPSHOT_SCENARIO");
        }
        if (payload.inputSnapshotVersion().startsWith("snapshot-v-verify")) {
            return new MockAssuranceResult(AssuranceResult.ASSURANCE_INCOMPLETE, "MOCK_VERIFICATION_REQUIRED");
        }
        return new MockAssuranceResult(AssuranceResult.ASSURANCE_COMPLETE, "GOVERNANCE_VERIFIED_PROPOSAL");
    }
}
