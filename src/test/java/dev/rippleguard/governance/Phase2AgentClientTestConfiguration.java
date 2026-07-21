package dev.rippleguard.governance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rippleguard.governance.application.LoanDecisionAgentClient;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class Phase2AgentClientTestConfiguration {
    @Bean
    @Primary
    LoanDecisionAgentClient testLoanDecisionAgentClient(ObjectMapper objectMapper) {
        return request -> {
            Map<String, Object> agentRun = new LinkedHashMap<>();
            agentRun.put("schemaVersion", "1.0.0");
            agentRun.put("decisionCaseId", request.get("decisionCaseId").asText());
            agentRun.put("evaluationRunId", request.get("evaluationRunId").asText());
            agentRun.put("agentRunId", request.get("agentRunId").asText());
            agentRun.put("attemptId", 1);
            agentRun.put("agentType", "LOAN_DECISION_AGENT");
            agentRun.put("requestIdempotencyKey", request.get("requestIdempotencyKey").asText());
            agentRun.put("startedAt", request.get("requestedAt").asText());
            agentRun.put("completedAt", request.get("requestedAt").asText());
            agentRun.put("runtimeVersion", "agent-runtime.test");

            Map<String, Object> proposal = new LinkedHashMap<>();
            proposal.put("schemaVersion", "1.0.0");
            proposal.put("proposalId", "70000000-0000-4000-8000-000000002001");
            proposal.put("proposalOutcome", "RECOMMEND_APPROVAL");
            proposal.put("repaymentLikelihoodScore", 0.86);
            proposal.put("scoreSemantics", "higher score means higher repayment likelihood");
            proposal.put("threshold", 0.72);
            proposal.put("thresholdVersion", request.get("thresholdVersion").asText());
            proposal.put("comparisonDirection", "score_greater_than_or_equal_threshold_supports_approval");
            proposal.put("reasonCodes", List.of("LOW_DTI", "STABLE_INCOME"));
            proposal.put("modelVersion", request.get("modelVersion").asText());
            proposal.put("featureSchemaVersion", request.get("featureSchemaVersion").asText());
            proposal.put("generatedAt", request.get("requestedAt").asText());

            Map<String, Object> result = new LinkedHashMap<>();
            result.put("schemaVersion", "1.0.0");
            result.put("resultStatus", "COMPLETED");
            result.put("agentRun", agentRun);
            result.put("snapshotReference", request.get("snapshotReference"));
            result.put("featureSchemaVersion", request.get("featureSchemaVersion").asText());
            result.put("preprocessingVersion", request.get("preprocessingVersion").asText());
            result.put("modelVersion", request.get("modelVersion").asText());
            result.put("modelArtifactDigest", request.get("modelArtifactDigest").asText());
            result.put("thresholdVersion", request.get("thresholdVersion").asText());
            result.put("proposal", proposal);
            result.put("explanationRef", "shap://loan-agent/test/attempt-1");
            result.put("explanationDigest", "sha256:2222222222222222222222222222222222222222222222222222222222222222");
            result.put("evidenceRefs", List.of(request.get("snapshotReference").get("snapshotReference").asText()));
            result.put("completedAt", request.get("requestedAt").asText());
            return objectMapper.valueToTree(result);
        };
    }
}
