package dev.rippleguard.governance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.rippleguard.governance.application.JsonSupport;
import dev.rippleguard.governance.application.LoanFeatureSnapshotClient;
import dev.rippleguard.governance.application.LoanFeatureSnapshotNotFoundException;
import dev.rippleguard.governance.application.LoanFeatureSnapshotTimeoutException;
import dev.rippleguard.governance.application.LoanDecisionAgentClient;
import dev.rippleguard.governance.application.Phase2FeatureSnapshot;
import dev.rippleguard.governance.infrastructure.agent.AgentRuntimeTimeoutException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class Phase2AgentClientTestConfiguration {
    @Bean
    @Primary
    LoanDecisionAgentClient testLoanDecisionAgentClient(ObjectMapper objectMapper) {
        return new RecordingLoanDecisionAgentClient(objectMapper);
    }

    @Bean
    @Primary
    LoanFeatureSnapshotClient testLoanFeatureSnapshotClient(ObjectMapper objectMapper, JsonSupport json) {
        return new RecordingLoanFeatureSnapshotClient(objectMapper, json);
    }

    static final class RecordingLoanFeatureSnapshotClient implements LoanFeatureSnapshotClient {
        private static final String FEATURE_SCHEMA_VERSION = "phase-2-loan-features.v1.0.0";
        private final ObjectMapper objectMapper;
        private final JsonSupport json;
        private final AtomicInteger calls = new AtomicInteger();
        private volatile boolean notFound;
        private volatile boolean timeout;
        private volatile boolean snapshotDigestMismatch;
        private volatile boolean featurePayloadDigestMismatch;
        private volatile boolean invalidFeaturePayloadContract;
        private volatile Phase2FeatureSnapshot lastSnapshot;

        RecordingLoanFeatureSnapshotClient(ObjectMapper objectMapper, JsonSupport json) {
            this.objectMapper = objectMapper;
            this.json = json;
        }

        @Override
        public Phase2FeatureSnapshot getByReference(java.util.UUID applicationId, String snapshotVersion) {
            calls.incrementAndGet();
            if (timeout) {
                throw new LoanFeatureSnapshotTimeoutException("test snapshot timeout", null);
            }
            if (notFound) {
                throw new LoanFeatureSnapshotNotFoundException("test snapshot not found");
            }
            Map<String, Object> features = new LinkedHashMap<>();
            features.put("annualIncome", 72000000);
            features.put("monthlyIncomeMean", 6000000);
            features.put("monthlyIncomeVolatility", 0.18);
            features.put("debtToIncomeRatio", 0.32);
            features.put("existingDebtAmount", 18000000);
            features.put("delinquencyCount", 0);
            features.put("platformSettlementMonths", 36);
            features.put("platformSettlementMean", 5200000);
            features.put("platformSettlementVolatility", 0.21);
            features.put("contractDurationMonths", 48);
            features.put("incomeDeclarationAvailable", true);
            features.put("telecomPaymentDelinquencyCount", 0);
            if (invalidFeaturePayloadContract) {
                features.remove("annualIncome");
            }
            Map<String, Object> featurePayloadWithoutDigest = new LinkedHashMap<>();
            featurePayloadWithoutDigest.put("schemaVersion", "1.0.0");
            featurePayloadWithoutDigest.put("featureSchemaVersion", FEATURE_SCHEMA_VERSION);
            featurePayloadWithoutDigest.put("features", features);
            String digest = json.sha256Prefixed(json.canonicalJson(featurePayloadWithoutDigest));
            Map<String, Object> featurePayload = new LinkedHashMap<>(featurePayloadWithoutDigest);
            featurePayload.put("featurePayloadDigest", featurePayloadDigestMismatch
                    ? "sha256:9999999999999999999999999999999999999999999999999999999999999999"
                    : digest);
            java.util.UUID snapshotId = java.util.UUID.nameUUIDFromBytes((applicationId + ":" + snapshotVersion).getBytes());
            Instant createdAt = Instant.parse("2026-07-21T01:00:00Z");
            Map<String, Object> snapshotReference = new LinkedHashMap<>();
            snapshotReference.put("schemaVersion", "1.0.0");
            snapshotReference.put("snapshotId", snapshotId.toString());
            snapshotReference.put("snapshotVersion", snapshotVersion);
            snapshotReference.put("snapshotSchemaVersion", "1.0.0");
            snapshotReference.put("snapshotCreatedAt", createdAt.toString());
            snapshotReference.put("digestAlgorithm", "sha256");
            snapshotReference.put("snapshotDigest", snapshotDigestMismatch
                    ? "sha256:8888888888888888888888888888888888888888888888888888888888888888"
                    : digest);
            snapshotReference.put("snapshotReference", "snapshot://loan-feature/" + applicationId + "/" + snapshotVersion);
            snapshotReference.put("referenceType", "MATERIALIZED_FEATURES");
            lastSnapshot = new Phase2FeatureSnapshot(
                    "1.0.0",
                    snapshotId,
                    applicationId,
                    snapshotVersion,
                    "1.0.0",
                    FEATURE_SCHEMA_VERSION,
                    objectMapper.valueToTree(snapshotReference),
                    objectMapper.valueToTree(featurePayload),
                    digest,
                    1,
                    createdAt
            );
            return lastSnapshot;
        }

        void reset() {
            calls.set(0);
            notFound = false;
            timeout = false;
            snapshotDigestMismatch = false;
            featurePayloadDigestMismatch = false;
            invalidFeaturePayloadContract = false;
            lastSnapshot = null;
        }

        int calls() {
            return calls.get();
        }

        Phase2FeatureSnapshot lastSnapshot() {
            return lastSnapshot;
        }

        void returnNotFound() {
            notFound = true;
        }

        void timeout() {
            timeout = true;
        }

        void mismatchSnapshotDigest() {
            snapshotDigestMismatch = true;
        }

        void mismatchFeaturePayloadDigest() {
            featurePayloadDigestMismatch = true;
        }

        void invalidFeaturePayloadContract() {
            invalidFeaturePayloadContract = true;
        }
    }

    static final class RecordingLoanDecisionAgentClient implements LoanDecisionAgentClient {
        private final ObjectMapper objectMapper;
        private final AtomicInteger calls = new AtomicInteger();
        private volatile int timeoutsBeforeSuccess;
        private volatile Integer attemptIdOverride;
        private volatile Long completedAtSecondsAfterDeadline;
        private volatile String snapshotFieldOverride;
        private volatile String snapshotValueOverride;
        private volatile boolean malformedResult;
        private volatile boolean retryableFailure;
        private volatile JsonNode lastRequest;

        RecordingLoanDecisionAgentClient(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public JsonNode execute(JsonNode request) {
            calls.incrementAndGet();
            lastRequest = request.deepCopy();
            if (timeoutsBeforeSuccess > 0) {
                timeoutsBeforeSuccess--;
                throw new AgentRuntimeTimeoutException("test timeout", null);
            }
            currentRequestedAt = request.get("requestedAt").asText();
            currentDeadlineAt = request.get("deadlineAt").asText();
            Map<String, Object> agentRun = new LinkedHashMap<>();
            agentRun.put("schemaVersion", "1.0.0");
            agentRun.put("decisionCaseId", request.get("decisionCaseId").asText());
            agentRun.put("evaluationRunId", request.get("evaluationRunId").asText());
            agentRun.put("agentRunId", request.get("agentRunId").asText());
            agentRun.put("attemptId", attemptIdOverride == null ? 1 : attemptIdOverride);
            agentRun.put("agentType", "LOAN_DECISION_AGENT");
            agentRun.put("requestIdempotencyKey", request.get("requestIdempotencyKey").asText());
            agentRun.put("startedAt", request.get("requestedAt").asText());
            String completedAt = completedAt();
            agentRun.put("completedAt", completedAt);
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
            result.put("resultStatus", retryableFailure ? "FAILED" : "COMPLETED");
            result.put("agentRun", agentRun);
            result.put("snapshotReference", request.get("snapshotReference"));
            result.put("featureSchemaVersion", request.get("featureSchemaVersion").asText());
            result.put("preprocessingVersion", request.get("preprocessingVersion").asText());
            result.put("modelVersion", request.get("modelVersion").asText());
            result.put("modelArtifactDigest", request.get("modelArtifactDigest").asText());
            result.put("thresholdVersion", request.get("thresholdVersion").asText());
            if (retryableFailure) {
                result.put("failure", Map.of(
                        "classification", "RETRYABLE",
                        "reasonCode", "AGENT_TIMEOUT",
                        "safeMessage", "Agent attempt timed out before deadline."
                ));
            } else {
                result.put("proposal", proposal);
                result.put("explanationRef", "shap://loan-agent/test/attempt-1");
                result.put("explanationDigest", "sha256:2222222222222222222222222222222222222222222222222222222222222222");
                result.put("evidenceRefs", List.of(request.get("snapshotReference").get("snapshotReference").asText()));
            }
            result.put("completedAt", completedAt);
            ObjectNode node = objectMapper.valueToTree(result);
            if (snapshotFieldOverride != null) {
                ((ObjectNode) node.get("snapshotReference")).put(snapshotFieldOverride, snapshotValueOverride);
            }
            if (malformedResult) {
                node.remove("resultStatus");
            }
            return node;
        }

        private String completedAt() {
            if (completedAtSecondsAfterDeadline != null) {
                return Instant.parse(currentDeadlineAt).plusSeconds(completedAtSecondsAfterDeadline).toString();
            }
            return currentRequestedAt;
        }

        int calls() {
            return calls.get();
        }

        void reset() {
            calls.set(0);
            timeoutsBeforeSuccess = 0;
            attemptIdOverride = null;
            completedAtSecondsAfterDeadline = null;
            snapshotFieldOverride = null;
            snapshotValueOverride = null;
            malformedResult = false;
            retryableFailure = false;
            lastRequest = null;
        }

        void timeoutNextCalls(int count) {
            timeoutsBeforeSuccess = count;
        }

        private volatile String currentRequestedAt;
        private volatile String currentDeadlineAt;

        void overrideAttemptId(int attemptId) {
            attemptIdOverride = attemptId;
        }

        void completeAfterDeadline(long seconds) {
            completedAtSecondsAfterDeadline = seconds;
        }

        void overrideSnapshotField(String field, String value) {
            snapshotFieldOverride = field;
            snapshotValueOverride = value;
        }

        void returnMalformedResult() {
            malformedResult = true;
        }

        void returnRetryableFailure() {
            retryableFailure = true;
        }

        JsonNode lastRequest() {
            return lastRequest;
        }
    }
}
