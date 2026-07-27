package dev.rippleguard.governance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.rippleguard.governance.application.DecisionCaseService;
import dev.rippleguard.governance.application.EventEnvelope;
import dev.rippleguard.governance.application.LoanFeatureSnapshotTimeoutException;
import dev.rippleguard.governance.application.MockDecisionEvaluator;
import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.EvaluationRunStatus;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseRepository;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunEntity;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunRepository;
import dev.rippleguard.governance.infrastructure.persistence.GovernanceEventQuarantineRepository;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import dev.rippleguard.governance.infrastructure.contracts.ContractSchemaValidator;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.support.TransactionTemplate;

@SpringBootTest(properties = {
        "debug=false",
        "AGENT_RUNTIME_RECOVERY_DELAY_MS=600000"
})
@Import(Phase2AgentClientTestConfiguration.class)
class DecisionCaseServiceIntegrationTest {
    @Autowired
    DecisionCaseService service;

    @Autowired
    DecisionCaseRepository decisionCases;

    @Autowired
    EvaluationRunRepository evaluationRuns;

    @Autowired
    OutboxEventRepository outbox;

    @Autowired
    GovernanceEventQuarantineRepository quarantine;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    ContractSchemaValidator contracts;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    Phase2AgentClientTestConfiguration.RecordingLoanDecisionAgentClient agentClient;

    @Autowired
    Phase2AgentClientTestConfiguration.RecordingLoanFeatureSnapshotClient snapshotClient;

    @Autowired
    TransactionTemplate transactions;

    @BeforeEach
    void cleanDatabase() {
        agentClient.reset();
        snapshotClient.reset();
        jdbc.update("delete from evaluation_run");
        jdbc.update("delete from outbox_event");
        jdbc.update("delete from inbox_event");
        jdbc.update("delete from governance_event_quarantine");
        jdbc.update("delete from decision_case");
    }

    @Test
    void submittedEventCreatesCaseEvaluationAndAgentValidationOutbox() throws Exception {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000001");
        EventEnvelope submitted = submitted(applicationId);
        service.handleLoanApplicationSubmitted(submitted);

        var response = service.getByApplication(applicationId);

        assertThat(response.status()).isEqualTo(DecisionCaseStatus.PROPOSAL_READY);
        assertThat(response.evaluationRunId()).isNotNull();
        assertThat(response.evaluationRunStatus()).isEqualTo(EvaluationRunStatus.COMPLETED);
        assertThat(response.finalDecision()).isNull();
        assertThat(decisionCases.findByApplicationId(applicationId)).isPresent();
        assertThat(evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())).isPresent();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsExactlyInAnyOrder(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "governance.agent-result.validated.v2"
                );

        List<OutboxRow> rows = outboxRowsByCreatedAt();
        assertThat(rows).extracting(OutboxRow::eventType)
                .containsExactly(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "governance.agent-result.validated.v2"
                );
        assertThat(rows.get(0).createdAt()).isBefore(rows.get(1).createdAt());

        List<Instant> occurredAt = rows.stream()
                .map(row -> occurredAt(row.payload()))
                .toList();
        assertThat(occurredAt.get(0)).isBefore(occurredAt.get(1));

        Map<String, UUID> eventIdsByType = rows.stream()
                .collect(java.util.stream.Collectors.toMap(
                        OutboxRow::eventType,
                        row -> eventId(row.payload())
                ));
        assertThat(causationId(payloadFor(rows, "governance.review.started.v1")))
                .isEqualTo(submitted.eventId());
        UUID requestEventId = eventIdsByType.get("agent.evaluation.requested.v1");
        UUID agentRunId = evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow()
                .getAgentRunId();
        assertThat(causationId(payloadFor(rows, "agent.evaluation.requested.v1")))
                .isEqualTo(submitted.eventId());
        assertThat(causationId(payloadFor(rows, "governance.agent-result.validated.v2")))
                .isEqualTo(requestEventId);
        assertThat(causationId(payloadFor(rows, "governance.agent-result.validated.v2")))
                .isNotEqualTo(agentRunId);
        JsonNode validationEvent = objectMapper.readTree(payloadFor(rows, "governance.agent-result.validated.v2"));
        JsonNode validationPayload = validationEvent.get("payload");
        assertThat(validationEvent.get("schemaVersion").asText()).isEqualTo("2.0.0");
        assertThat(validationPayload.get("requestEventId").asText()).isEqualTo(requestEventId.toString());
        JsonNode snapshotReference = snapshotClient.lastSnapshot().snapshotReference();
        assertThat(validationPayload.get("snapshotId").asText()).isEqualTo(snapshotReference.path("snapshotId").asText());
        assertThat(validationPayload.get("snapshotVersion").asText()).isEqualTo(snapshotReference.path("snapshotVersion").asText());
        assertThat(validationPayload.get("snapshotSchemaVersion").asText())
                .isEqualTo(snapshotReference.path("snapshotSchemaVersion").asText());
        assertThat(validationPayload.get("snapshotDigest").asText())
                .isEqualTo(snapshotReference.path("snapshotDigest").asText());
        assertThat(validationPayload.get("featureSchemaVersion").asText()).isEqualTo("phase-2-loan-features.v1.0.0");
        assertThat(validationPayload.get("preprocessingVersion").asText()).isEqualTo("preprocess.v1.0.0");
        assertThat(validationPayload.get("modelVersion").asText()).isEqualTo("loan-model.v1.0.0");
        assertThat(validationPayload.get("modelArtifactDigest").asText())
                .isEqualTo("sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        assertThat(validationPayload.get("thresholdVersion").asText()).isEqualTo("threshold.v1.0.0");
        assertThat(validationPayload.get("agentResultReference").asText())
                .startsWith("agent-result://" + response.caseId() + "/" + agentRunId + "/attempt-");
        assertThat(validationPayload.get("agentResultDigest").asText()).startsWith("sha256:");
        assertThat(agentClient.lastRequest().hasNonNull("featurePayload")).isTrue();
        EvaluationRunEntity run = evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(run.getRequestEventId()).isEqualTo(requestEventId);
        assertThat(run.getFeaturePayloadDigest()).isEqualTo(snapshotClient.lastSnapshot().featurePayloadDigest());
        assertThat(run.getFeaturePayload()).contains("featurePayloadDigest");
    }

    @Test
    void duplicateSubmittedEventIsIdempotent() {
        EventEnvelope event = submitted(UUID.fromString("10000000-0000-4000-8000-000000000002"));

        service.handleLoanApplicationSubmitted(event);
        service.handleLoanApplicationSubmitted(event);

        assertThat(decisionCases.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(3);
        assertThat(snapshotClient.calls()).isEqualTo(1);
    }

    @Test
    void duplicateSubmittedEventResumesRunningEvaluationAfterCrashWindow() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000008");
        EventEnvelope event = submitted(applicationId);
        agentClient.timeoutNextCalls(1);

        service.handleLoanApplicationSubmitted(event);

        var firstResponse = service.getByApplication(applicationId);
        assertThat(firstResponse.status()).isEqualTo(DecisionCaseStatus.EVALUATION_REQUESTED);
        EvaluationRunEntity pendingRun = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(firstResponse.caseId())
                .orElseThrow();
        assertThat(pendingRun.getStatus()).isEqualTo(EvaluationRunStatus.RUNNING);
        assertThat(pendingRun.getAttemptCount()).isEqualTo(1);
        assertThat(jdbc.queryForObject("select count(*) from inbox_event", Long.class)).isEqualTo(1);
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1", "agent.evaluation.requested.v1");

        jdbc.update(
                "update evaluation_run set next_attempt_at = ?, lease_until = null where evaluation_run_id = ?",
                Timestamp.from(Instant.now().minusSeconds(1)),
                pendingRun.getEvaluationRunId()
        );
        service.handleLoanApplicationSubmitted(event);

        var recoveredResponse = service.getByApplication(applicationId);
        assertThat(recoveredResponse.status()).isEqualTo(DecisionCaseStatus.PROPOSAL_READY);
        EvaluationRunEntity completedRun = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(recoveredResponse.caseId())
                .orElseThrow();
        assertThat(completedRun.getStatus()).isEqualTo(EvaluationRunStatus.COMPLETED);
        assertThat(completedRun.getAttemptCount()).isEqualTo(2);
        assertThat(agentClient.calls()).isEqualTo(2);
        assertThat(outbox.findAll()).extracting("eventType")
                .containsExactlyInAnyOrder(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "governance.agent-result.validated.v2"
                );
        assertThat(completedRun.getSnapshotId()).isEqualTo(snapshotClient.lastSnapshot().snapshotId().toString());
        assertThat(completedRun.getAgentRunId()).isEqualTo(pendingRun.getAgentRunId());
    }

    @Test
    void schedulerFailsRunWhenFinalClaimedAttemptLeaseExpiresBeforeRuntimeCall() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000009");
        EventEnvelope event = submitted(applicationId);
        agentClient.timeoutNextCalls(1);

        service.handleLoanApplicationSubmitted(event);

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.RUNNING);
        assertThat(run.getAttemptCount()).isEqualTo(1);

        jdbc.update(
                """
                update evaluation_run
                set attempt_count = 2,
                    next_attempt_at = ?,
                    lease_until = ?
                where evaluation_run_id = ?
                """,
                Timestamp.from(Instant.now().minusSeconds(5)),
                Timestamp.from(Instant.now().minusSeconds(1)),
                run.getEvaluationRunId()
        );

        service.resumePendingPhase2AgentRuns();

        EvaluationRunEntity recoveredRun = evaluationRuns.findById(run.getEvaluationRunId()).orElseThrow();
        assertThat(recoveredRun.getStatus()).isEqualTo(EvaluationRunStatus.FAILED);
        assertThat(recoveredRun.getAttemptCount()).isEqualTo(2);
        assertThat(recoveredRun.getFailureReasonCode()).isEqualTo("RETRY_EXHAUSTED");
        assertThat(service.getByApplication(applicationId).status())
                .isEqualTo(DecisionCaseStatus.VERIFICATION_REQUIRED);
        assertThat(agentClient.calls()).isEqualTo(1);
    }

    @Test
    void cachedEarlierRuntimeAttemptCanSatisfyClaimedRetryAttempt() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000010");
        EventEnvelope event = submitted(applicationId);
        agentClient.timeoutNextCalls(1);

        service.handleLoanApplicationSubmitted(event);

        var firstResponse = service.getByApplication(applicationId);
        EvaluationRunEntity pendingRun = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(firstResponse.caseId())
                .orElseThrow();
        jdbc.update(
                "update evaluation_run set next_attempt_at = ?, lease_until = null where evaluation_run_id = ?",
                Timestamp.from(Instant.now().minusSeconds(1)),
                pendingRun.getEvaluationRunId()
        );

        service.handleLoanApplicationSubmitted(event);

        var recoveredResponse = service.getByApplication(applicationId);
        EvaluationRunEntity completedRun = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(recoveredResponse.caseId())
                .orElseThrow();
        assertThat(completedRun.getStatus()).isEqualTo(EvaluationRunStatus.COMPLETED);
        assertThat(completedRun.getAttemptCount()).isEqualTo(2);
        JsonNode firstRequest = agentClient.requestAt(0);
        JsonNode retriedRequest = agentClient.requestAt(1);
        assertThat(retriedRequest.path("snapshotReference")).isEqualTo(firstRequest.path("snapshotReference"));
        assertThat(retriedRequest.path("featureSchemaVersion").asText())
                .isEqualTo(firstRequest.path("featureSchemaVersion").asText());
        assertThat(retriedRequest.path("preprocessingVersion").asText())
                .isEqualTo(firstRequest.path("preprocessingVersion").asText());
        assertThat(retriedRequest.path("modelVersion").asText()).isEqualTo(firstRequest.path("modelVersion").asText());
        assertThat(retriedRequest.path("modelArtifactDigest").asText())
                .isEqualTo(firstRequest.path("modelArtifactDigest").asText());
        assertThat(retriedRequest.path("thresholdVersion").asText())
                .isEqualTo(firstRequest.path("thresholdVersion").asText());
        assertThat(retriedRequest.path("agentRunId").asText()).isEqualTo(firstRequest.path("agentRunId").asText());

        JsonNode validationEvent = validationEvent(outboxRowsByCreatedAt());
        assertValidValidationEventContract(validationEvent);
        JsonNode validationPayload = validationEvent.get("payload");
        assertThat(validationReasonCodes(validationEvent.toString()))
                .containsExactly("SCHEMA_VALID", "MODEL_PROVENANCE_VALID", "SNAPSHOT_MATCHED", "SHAP_PRESENT");
        assertValidationPayloadMatchesRunProvenance(validationPayload, completedRun);
        assertThat(validationPayload.path("snapshotDigest").asText())
                .isEqualTo(firstRequest.path("snapshotReference").path("snapshotDigest").asText());
        assertThat(validationPayload.path("modelVersion").asText()).isEqualTo(firstRequest.path("modelVersion").asText());
        assertThat(validationPayload.path("modelArtifactDigest").asText())
                .isEqualTo(firstRequest.path("modelArtifactDigest").asText());
        assertThat(validationPayload.path("preprocessingVersion").asText())
                .isEqualTo(firstRequest.path("preprocessingVersion").asText());
        assertThat(validationPayload.path("thresholdVersion").asText())
                .isEqualTo(firstRequest.path("thresholdVersion").asText());
        assertThat(validationPayload.path("agentRunId").asText()).isEqualTo(firstRequest.path("agentRunId").asText());
    }

    @Test
    void rejectsRuntimeAttemptBeyondClaimedAttempt() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000011");
        agentClient.overrideAttemptId(2);

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.BLOCKED);
        assertThat(run.getFailureReasonCode()).isEqualTo("AGENT_ATTEMPT_MISMATCH");
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1", "agent.evaluation.requested.v1");
    }

    @Test
    void retryableFailedResultWithFutureAttemptIsBlockedBeforeRetryScheduling() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000018");
        agentClient.returnRetryableFailure();
        agentClient.overrideAttemptId(2);

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.BLOCKED);
        assertThat(run.getFailureReasonCode()).isEqualTo("AGENT_ATTEMPT_MISMATCH");
        assertThat(run.getAttemptCount()).isEqualTo(1);
        assertThat(run.getLastTransportFailureCode()).isNull();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1", "agent.evaluation.requested.v1");
    }

    @Test
    void retryableFailedResultWithSnapshotMismatchIsBlockedBeforeRetryScheduling() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000019");
        agentClient.returnRetryableFailure();
        agentClient.overrideSnapshotField("snapshotDigest",
                "sha256:9999999999999999999999999999999999999999999999999999999999999999");

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.BLOCKED);
        assertThat(run.getFailureReasonCode()).isEqualTo("SNAPSHOT_IDENTITY_MISMATCH");
        assertThat(run.getAttemptCount()).isEqualTo(1);
        assertThat(run.getLastTransportFailureCode()).isNull();
    }

    @Test
    void snapshotApiNotFoundCreatesVerificationRequiredWithoutFallbackProposal() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000020");
        snapshotClient.returnNotFound();

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.VERIFICATION_REQUIRED);
        assertThat(response.evaluationRunId()).isNull();
        assertThat(agentClient.calls()).isZero();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1");
    }

    @Test
    void snapshotApiTimeoutIsRetryableBeforePersistence() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000021");
        snapshotClient.timeout();

        assertThatThrownBy(() -> service.handleLoanApplicationSubmitted(submitted(applicationId)))
                .isInstanceOf(LoanFeatureSnapshotTimeoutException.class);

        assertThat(decisionCases.count()).isZero();
        assertThat(outbox.count()).isZero();
        assertThat(agentClient.calls()).isZero();
    }

    @Test
    void snapshotDigestMismatchBlocksWithoutMockFallback() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000022");
        snapshotClient.mismatchSnapshotDigest();

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(response.evaluationRunId()).isNull();
        assertThat(agentClient.calls()).isZero();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1");
    }

    @Test
    void featurePayloadDigestMismatchBlocksWithoutRuntimeCall() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000023");
        snapshotClient.mismatchFeaturePayloadDigest();

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(response.evaluationRunId()).isNull();
        assertThat(agentClient.calls()).isZero();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1");
    }

    @Test
    void invalidFeaturePayloadContractRequiresValidationWithoutRuntimeCall() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000024");
        snapshotClient.invalidFeaturePayloadContract();

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.VERIFICATION_REQUIRED);
        assertThat(response.evaluationRunId()).isNull();
        assertThat(agentClient.calls()).isZero();
    }

    @Test
    void rejectsCompletedResultAfterEvaluationDeadline() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000012");
        agentClient.completeAfterDeadline(30);

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.VERIFICATION_REQUIRED);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.FAILED);
        assertThat(run.getFailureReasonCode()).isEqualTo("DEADLINE_EXCEEDED");
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1", "agent.evaluation.requested.v1");
    }

    @Test
    void malformedRuntimeResultRecordsSchemaInvalidWithoutSyntheticAgentFailure() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000017");
        agentClient.returnMalformedResult();

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.VERIFICATION_REQUIRED);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.FAILED);
        assertThat(run.getFailureReasonCode()).isEqualTo("CONTRACT_VALIDATION_FAILED");
        assertThat(outbox.findAll()).extracting("eventType")
                .containsExactlyInAnyOrder(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "governance.agent-result.validated.v2"
                );
        assertThat(countByEventType("governance.agent-result.validated.v2")).isEqualTo(1);
        JsonNode validationEvent = validationEvent(outboxRowsByCreatedAt());
        assertValidValidationEventContract(validationEvent);
        JsonNode validationPayload = validationEvent.get("payload");
        assertThat(validationPayload.path("validationOutcome").asText()).isEqualTo("REJECTED");
        assertThat(validationReasonCodes(validationEvent.toString()))
                .containsExactly("SCHEMA_INVALID");
        assertValidationPayloadMatchesRunProvenance(validationPayload, run);
        assertThat(validationPayload.path("agentResultReference").asText())
                .startsWith("agent-result://" + response.caseId() + "/" + run.getAgentRunId() + "/attempt-");
        assertThat(validationPayload.path("agentResultDigest").asText()).startsWith("sha256:");
    }

    @Test
    void missingPersistedProvenanceBlocksWithoutValidationEvent() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000021");
        EventEnvelope event = submitted(applicationId);
        agentClient.timeoutNextCalls(1);

        service.handleLoanApplicationSubmitted(event);

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        jdbc.update(
                "update evaluation_run set request_event_id = null, next_attempt_at = ?, lease_until = null where evaluation_run_id = ?",
                Timestamp.from(Instant.now().minusSeconds(1)),
                run.getEvaluationRunId()
        );

        service.handleLoanApplicationSubmitted(event);

        EvaluationRunEntity blockedRun = evaluationRuns.findById(run.getEvaluationRunId()).orElseThrow();
        assertThat(blockedRun.getStatus()).isEqualTo(EvaluationRunStatus.BLOCKED);
        assertThat(blockedRun.getFailureReasonCode()).isEqualTo("VALIDATION_EVENT_PROVENANCE_MISSING");
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1", "agent.evaluation.requested.v1");
    }

    @Test
    void rejectsSnapshotIdentityMismatchBeyondDigest() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000013");
        agentClient.overrideSnapshotField("snapshotId", "snapshot-other");

        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.BLOCKED);
        assertThat(run.getFailureReasonCode()).isEqualTo("SNAPSHOT_IDENTITY_MISMATCH");
    }

    @Test
    void duplicateAcceptedResultDigestIsNoOp() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000014");
        service.handleLoanApplicationSubmitted(submitted(applicationId));
        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(run.getRequestedAt()).isEqualTo(run.getRequestedAt().truncatedTo(ChronoUnit.MICROS));
        assertThat(run.getDeadlineAt()).isEqualTo(run.getDeadlineAt().truncatedTo(ChronoUnit.MICROS));
        long outboxCount = outbox.count();

        recordResultAgain(run, false);

        EvaluationRunEntity unchangedRun = evaluationRuns.findById(run.getEvaluationRunId()).orElseThrow();
        assertThat(unchangedRun.getStatus()).isEqualTo(EvaluationRunStatus.COMPLETED);
        assertThat(service.getByApplication(applicationId).status()).isEqualTo(DecisionCaseStatus.PROPOSAL_READY);
        assertThat(outbox.count()).isEqualTo(outboxCount);
    }

    @Test
    void conflictingAcceptedResultDigestBlocksRunWithoutUnrepresentableValidationEvent() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000015");
        service.handleLoanApplicationSubmitted(submitted(applicationId));
        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        long outboxCount = outbox.count();

        recordResultAgain(run, true);

        EvaluationRunEntity blockedRun = evaluationRuns.findById(run.getEvaluationRunId()).orElseThrow();
        assertThat(blockedRun.getStatus()).isEqualTo(EvaluationRunStatus.BLOCKED);
        assertThat(blockedRun.getFailureReasonCode()).isEqualTo("AGENT_RUN_RESULT_CONFLICT");
        var blockedResponse = service.getByApplication(applicationId);
        assertThat(blockedResponse.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(blockedResponse.proposal()).isNull();
        assertThat(outbox.count()).isEqualTo(outboxCount);
    }

    @Test
    void mockEvaluationIsDeterministic() {
        MockDecisionEvaluator evaluator = new MockDecisionEvaluator();
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000003");

        var first = evaluator.evaluate(applicationId, "case-10000000", "snapshot-v1");
        var second = evaluator.evaluate(applicationId, "case-10000000", "snapshot-v1");

        assertThat(second).isEqualTo(first);
    }

    @Test
    void quarantinesUnsupportedSchemaVersion() {
        EventEnvelope event = submitted(UUID.fromString("10000000-0000-4000-8000-000000000004"));
        EventEnvelope badVersion = new EventEnvelope(
                event.eventId(),
                event.eventType(),
                "1.0.0",
                event.occurredAt(),
                event.producer(),
                event.applicationId(),
                event.caseId(),
                event.evaluationRunId(),
                event.correlationId(),
                event.causationId(),
                event.payload()
        );

        service.handleLoanApplicationSubmitted(badVersion);

        assertThat(decisionCases.count()).isZero();
        assertThat(quarantine.count()).isEqualTo(1);
    }

    @Test
    void missingSnapshotCreatesVerificationRequiredCaseAndNoDecisionCommand() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000005");
        EventEnvelope event = event(applicationId, Map.of(
                "applicationId", applicationId.toString(),
                "applicantId", "customer-42",
                "submittedAt", Instant.now().toString(),
                "submissionChannel", "WEB"
        ));

        service.handleLoanApplicationSubmitted(event);

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.VERIFICATION_REQUIRED);
        assertThat(response.evaluationRunId()).isNull();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsOnly("governance.review.started.v1");
    }

    @Test
    void phase2DoesNotUseSnapshotNameToForceMockBlockedDecision() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000006");

        service.handleLoanApplicationSubmitted(submitted(applicationId, "snapshot-v-blocked-001"));

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.PROPOSAL_READY);
        assertThat(outbox.findAll()).extracting("eventType")
                .doesNotContain("loan.decision.commanded.v1");
    }

    @Test
    void phase2SnapshotCreatedAtUsesDatabasePrecisionForIdentityComparison() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000016");
        Instant nanosecondOccurredAt = Instant.parse("2026-07-21T08:40:17.123456789Z");

        service.handleLoanApplicationSubmitted(submitted(applicationId, "snapshot-v1", nanosecondOccurredAt));

        var response = service.getByApplication(applicationId);
        EvaluationRunEntity run = evaluationRuns
                .findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())
                .orElseThrow();
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.PROPOSAL_READY);
        assertThat(run.getStatus()).isEqualTo(EvaluationRunStatus.COMPLETED);
        assertThat(outbox.findAll()).extracting("eventType")
                .contains("governance.agent-result.validated.v2");
    }

    @Test
    void conflictingSubmittedEventMarksRecalculationRequired() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000007");

        service.handleLoanApplicationSubmitted(submitted(applicationId, "snapshot-v1"));
        service.handleLoanApplicationSubmitted(submitted(applicationId, "snapshot-v-reject-001"));

        assertThat(service.getByApplication(applicationId).status())
                .isEqualTo(DecisionCaseStatus.RECALCULATION_REQUIRED);
    }

    private EventEnvelope submitted(UUID applicationId) {
        return submitted(applicationId, "snapshot-v1");
    }

    private EventEnvelope submitted(UUID applicationId, String snapshotVersion) {
        return submitted(applicationId, snapshotVersion, Instant.now());
    }

    private EventEnvelope submitted(UUID applicationId, String snapshotVersion, Instant occurredAt) {
        return event(applicationId, Map.of(
                "applicationId", applicationId.toString(),
                "applicantId", "customer-42",
                "inputSnapshotVersion", snapshotVersion,
                "submittedAt", Instant.now().toString(),
                "submissionChannel", "WEB"
        ), occurredAt);
    }

    private EventEnvelope event(UUID applicationId, Map<String, Object> payload) {
        return event(applicationId, payload, Instant.now());
    }

    private EventEnvelope event(UUID applicationId, Map<String, Object> payload, Instant occurredAt) {
        return new EventEnvelope(
                UUID.randomUUID(),
                "loan.application.submitted.v1",
                "1.1.0",
                occurredAt,
                "loan-service",
                applicationId,
                applicationId.toString(),
                null,
                applicationId.toString(),
                null,
                objectMapper.valueToTree(payload)
        );
    }

    private void recordResultAgain(EvaluationRunEntity run, boolean conflicting) {
        transactions.executeWithoutResult(status -> {
            EvaluationRunEntity attachedRun = evaluationRuns.findById(run.getEvaluationRunId()).orElseThrow();
            Object execution = ReflectionTestUtils.invokeMethod(service, "executionFromRun", attachedRun);
            JsonNode request = ReflectionTestUtils.invokeMethod(execution, "request");
            ObjectNode result = (ObjectNode) agentClient.execute(request);
            if (conflicting) {
                result.put("explanationDigest", "sha256:3333333333333333333333333333333333333333333333333333333333333333");
            }
            ReflectionTestUtils.invokeMethod(service, "validateAndRecordResult", execution, result, 1);
        });
    }

    private List<OutboxRow> outboxRowsByCreatedAt() {
        return jdbc.query(
                "select event_type, payload, created_at from outbox_event order by created_at",
                (rs, rowNum) -> new OutboxRow(
                        rs.getString("event_type"),
                        rs.getString("payload"),
                        toInstant(rs.getObject("created_at"))
                )
        );
    }

    private String payloadFor(List<OutboxRow> rows, String eventType) {
        return rows.stream()
                .filter(row -> row.eventType().equals(eventType))
                .findFirst()
                .orElseThrow()
                .payload();
    }

    private JsonNode validationEvent(List<OutboxRow> rows) {
        try {
            return objectMapper.readTree(payloadFor(rows, "governance.agent-result.validated.v2"));
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private void assertValidValidationEventContract(JsonNode validationEvent) {
        assertThatCode(() -> contracts.validate("events/governance.agent-result.validated.v2.0.0.schema.json",
                validationEvent))
                .doesNotThrowAnyException();
    }

    private void assertValidationPayloadMatchesRunProvenance(JsonNode payload, EvaluationRunEntity run) {
        assertThat(payload.path("snapshotId").asText()).isEqualTo(run.getSnapshotId());
        assertThat(payload.path("snapshotVersion").asText()).isEqualTo(run.getInputSnapshotVersion());
        assertThat(payload.path("snapshotSchemaVersion").asText()).isEqualTo(run.getSnapshotSchemaVersion());
        assertThat(payload.path("snapshotDigest").asText()).isEqualTo(run.getSnapshotDigest());
        assertThat(payload.path("featureSchemaVersion").asText()).isEqualTo(run.getFeatureSchemaVersion());
        assertThat(payload.path("preprocessingVersion").asText()).isEqualTo(run.getPreprocessingVersion());
        assertThat(payload.path("modelVersion").asText()).isEqualTo(run.getModelVersion());
        assertThat(payload.path("modelArtifactDigest").asText()).isEqualTo(run.getModelArtifactDigest());
        assertThat(payload.path("thresholdVersion").asText()).isEqualTo(run.getThresholdVersion());
        assertThat(payload.path("requestEventId").asText()).isEqualTo(run.getRequestEventId().toString());
        assertThat(payload.path("agentRunId").asText()).isEqualTo(run.getAgentRunId().toString());
    }

    private Long countByEventType(String eventType) {
        return jdbc.queryForObject("select count(*) from outbox_event where event_type = ?", Long.class, eventType);
    }

    private Instant occurredAt(String payload) {
        try {
            return Instant.parse(objectMapper.readTree(payload).get("occurredAt").asText());
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private UUID eventId(String payload) {
        try {
            return UUID.fromString(objectMapper.readTree(payload).get("eventId").asText());
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private UUID causationId(String payload) {
        try {
            var value = objectMapper.readTree(payload).get("causationId");
            return value == null || value.isNull() ? null : UUID.fromString(value.asText());
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private List<String> validationReasonCodes(String payload) {
        try {
            List<String> reasonCodes = new ArrayList<>();
            objectMapper.readTree(payload).get("payload").get("validationReasonCodes")
                    .forEach(reasonCode -> reasonCodes.add(reasonCode.asText()));
            return reasonCodes;
        } catch (Exception exception) {
            throw new IllegalStateException(exception);
        }
    }

    private Instant toInstant(Object value) {
        if (value instanceof Instant instant) {
            return instant;
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toInstant();
        }
        if (value instanceof Timestamp timestamp) {
            return timestamp.toInstant();
        }
        throw new IllegalArgumentException("Unsupported timestamp type: " + value.getClass());
    }

    private record OutboxRow(String eventType, String payload, Instant createdAt) {
    }
}
