package dev.rippleguard.governance.application;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import dev.rippleguard.governance.domain.AssuranceResult;
import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.EvaluationRunStatus;
import dev.rippleguard.governance.domain.QuarantineFailureCode;
import dev.rippleguard.governance.infrastructure.agent.AgentRuntimeTimeoutException;
import dev.rippleguard.governance.infrastructure.agent.AgentRuntimeTransportException;
import dev.rippleguard.governance.infrastructure.contracts.ContractSchemaValidator;
import dev.rippleguard.governance.infrastructure.contracts.ContractValidationException;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseEntity;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseRepository;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunEntity;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunRepository;
import dev.rippleguard.governance.infrastructure.persistence.GovernanceEventQuarantineEntity;
import dev.rippleguard.governance.infrastructure.persistence.GovernanceEventQuarantineRepository;
import dev.rippleguard.governance.infrastructure.persistence.InboxEventEntity;
import dev.rippleguard.governance.infrastructure.persistence.InboxEventRepository;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventEntity;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import dev.rippleguard.governance.interfaces.rest.DecisionCaseResponse;
import java.time.Clock;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class DecisionCaseService {
    private static final Logger log = LoggerFactory.getLogger(DecisionCaseService.class);
    private static final String LOAN_SUBMITTED_SCHEMA_VERSION = "1.1.0";
    private static final String GOVERNANCE_AGENT_EVENT_SCHEMA_VERSION = "1.0.0";
    private static final String REQUESTED_EVENT_SCHEMA_VERSION = "1.1.0";
    private static final String REQUEST_SCHEMA = "commands/loan-decision-agent-request.v1.0.0.schema.json";
    private static final String RESULT_SCHEMA = "agent-output/loan-decision-agent-result.v1.0.0.schema.json";
    private static final String SNAPSHOT_REFERENCE_SCHEMA = "domain/snapshot-reference.v1.0.0.schema.json";
    private static final String FEATURE_PAYLOAD_SCHEMA = "domain/feature-payload.v1.0.0.schema.json";
    private static final String REQUESTED_EVENT_SCHEMA = "events/agent.evaluation.requested.v1.1.0.schema.json";
    private static final String VALIDATED_EVENT_SCHEMA = "events/governance.agent-result.validated.v1.0.0.schema.json";

    private final DecisionCaseRepository decisionCases;
    private final EvaluationRunRepository evaluationRuns;
    private final InboxEventRepository inbox;
    private final OutboxEventRepository outbox;
    private final GovernanceEventQuarantineRepository quarantine;
    private final JsonSupport json;
    private final LoanFeatureSnapshotClient snapshots;
    private final LoanDecisionAgentClient agentClient;
    private final ContractSchemaValidator contracts;
    private final Phase2ExecutionPlanProperties executionPlan;
    private final Clock clock;
    private final TransactionTemplate transactions;
    private final String instanceId = "governance-" + UUID.randomUUID();

    public DecisionCaseService(DecisionCaseRepository decisionCases,
                               EvaluationRunRepository evaluationRuns,
                               InboxEventRepository inbox,
                               OutboxEventRepository outbox,
                               GovernanceEventQuarantineRepository quarantine,
                               JsonSupport json,
                               LoanFeatureSnapshotClient snapshots,
                               LoanDecisionAgentClient agentClient,
                               ContractSchemaValidator contracts,
                               Phase2ExecutionPlanProperties executionPlan,
                               Clock clock,
                               TransactionTemplate transactions) {
        this.decisionCases = decisionCases;
        this.evaluationRuns = evaluationRuns;
        this.inbox = inbox;
        this.outbox = outbox;
        this.quarantine = quarantine;
        this.json = json;
        this.snapshots = snapshots;
        this.agentClient = agentClient;
        this.contracts = contracts;
        this.executionPlan = executionPlan;
        this.clock = clock;
        this.transactions = transactions;
    }

    public void handleLoanApplicationSubmitted(EventEnvelope event) {
        SnapshotAcquisition snapshot = snapshotAcquisitionFor(event);
        AgentExecution execution;
        try {
            execution = transactions.execute(status -> planAgentExecution(event, snapshot));
        } catch (DataIntegrityViolationException conflict) {
            execution = recoverPlanningConflict(event, conflict);
        } catch (OptimisticLockingFailureException conflict) {
            execution = recoverPlanningConflict(event, conflict);
        }
        if (execution == null || execution.alreadyProcessed()) {
            return;
        }

        executeAgentRun(execution);
    }

    private SnapshotAcquisition snapshotAcquisitionFor(EventEnvelope event) {
        if (!supportsEvent(event, "loan.application.submitted.v1", LOAN_SUBMITTED_SCHEMA_VERSION)
                || inbox.existsById(event.eventId())) {
            return SnapshotAcquisition.notNeeded();
        }
        LoanApplicationSubmittedPayload payload =
                json.fromJson(event.payload().toString(), LoanApplicationSubmittedPayload.class);
        validateEnvelope(event, payload);
        if (payload.inputSnapshotVersion() == null || payload.inputSnapshotVersion().isBlank()
                || decisionCases.findByApplicationId(payload.applicationId()).isPresent()) {
            return SnapshotAcquisition.notNeeded();
        }
        try {
            Phase2FeatureSnapshot snapshot = snapshots.getByReference(payload.applicationId(), payload.inputSnapshotVersion());
            verifySnapshotPayload(event, payload, snapshot);
            return SnapshotAcquisition.available(snapshot);
        } catch (LoanFeatureSnapshotNotFoundException exception) {
            return SnapshotAcquisition.failed("VERIFICATION_REQUIRED", "FEATURE_SNAPSHOT_NOT_FOUND");
        } catch (LoanFeatureSnapshotAccessDeniedException exception) {
            return SnapshotAcquisition.failed("BLOCKED", "FEATURE_SNAPSHOT_ACCESS_DENIED");
        } catch (LoanFeatureSnapshotRequestRejectedException exception) {
            return SnapshotAcquisition.failed("VALIDATION_REQUIRED", "FEATURE_SNAPSHOT_REQUEST_REJECTED");
        } catch (ContractValidationException exception) {
            return SnapshotAcquisition.failed("VALIDATION_REQUIRED", "FEATURE_PAYLOAD_CONTRACT_INVALID");
        } catch (SnapshotVerificationException exception) {
            return SnapshotAcquisition.failed(exception.classification(), exception.reasonCode());
        }
    }

    private void verifySnapshotPayload(EventEnvelope event, LoanApplicationSubmittedPayload payload,
                                       Phase2FeatureSnapshot snapshot) {
        if (snapshot == null) {
            throw new LoanFeatureSnapshotTransportException("Loan Service returned an empty feature snapshot", null);
        }
        if (!"1.0.0".equals(snapshot.schemaVersion())
                || !payload.applicationId().equals(snapshot.applicationId())
                || !payload.inputSnapshotVersion().equals(snapshot.snapshotVersion())
                || !executionPlan.featureSchemaVersion().equals(snapshot.featureSchemaVersion())) {
            throw new SnapshotVerificationException("BLOCKED", "FEATURE_SNAPSHOT_IDENTITY_MISMATCH",
                    "Loan feature snapshot identity did not match submitted event");
        }
        JsonNode snapshotReference = snapshot.snapshotReference();
        JsonNode featurePayload = snapshot.featurePayload();
        contracts.validate(SNAPSHOT_REFERENCE_SCHEMA, snapshotReference);
        contracts.validate(FEATURE_PAYLOAD_SCHEMA, featurePayload);
        if (!snapshot.snapshotId().toString().equals(snapshotReference.path("snapshotId").asText())
                || !snapshot.snapshotVersion().equals(snapshotReference.path("snapshotVersion").asText())
                || !snapshot.snapshotSchemaVersion().equals(snapshotReference.path("snapshotSchemaVersion").asText())
                || !snapshot.createdAt().equals(parseInstant(snapshotReference.path("snapshotCreatedAt").asText()))
                || !snapshot.featurePayloadDigest().equals(snapshotReference.path("snapshotDigest").asText())
                || !"MATERIALIZED_FEATURES".equals(snapshotReference.path("referenceType").asText())) {
            throw new SnapshotVerificationException("BLOCKED", "SNAPSHOT_DIGEST_MISMATCH",
                    "Loan feature snapshot reference fields did not match API identity");
        }
        String declaredFeatureDigest = featurePayload.path("featurePayloadDigest").asText(null);
        if (!snapshot.featurePayloadDigest().equals(declaredFeatureDigest)) {
            throw new SnapshotVerificationException("BLOCKED", "FEATURE_PAYLOAD_DIGEST_MISMATCH",
                    "Feature payload digest did not match API digest");
        }
        Map<String, Object> digestInput = json.fromJson(featurePayload.toString(), Map.class);
        digestInput.remove("featurePayloadDigest");
        String actualFeatureDigest = json.sha256Prefixed(json.canonicalJson(digestInput));
        if (!snapshot.featurePayloadDigest().equals(actualFeatureDigest)) {
            throw new SnapshotVerificationException("BLOCKED", "FEATURE_PAYLOAD_DIGEST_MISMATCH",
                    "Feature payload canonical digest mismatch for event " + event.eventId());
        }
    }

    @Scheduled(fixedDelayString = "${AGENT_RUNTIME_RECOVERY_DELAY_MS:5000}")
    public void resumePendingPhase2AgentRuns() {
        List<AgentExecution> executions = transactions.execute(status ->
                evaluationRuns.findResumablePhase2Runs(clock.instant()).stream()
                        .limit(10)
                        .map(this::executionFromRun)
                        .toList()
        );
        if (executions == null) {
            return;
        }
        executions.forEach(this::executeAgentRun);
    }

    private AgentExecution planAgentExecution(EventEnvelope event, SnapshotAcquisition snapshot) {
        if (!supportsEvent(event, "loan.application.submitted.v1", LOAN_SUBMITTED_SCHEMA_VERSION)) {
            quarantine(event, QuarantineFailureCode.UNSUPPORTED_SCHEMA_VERSION,
                    "Unsupported event contract: " + event.eventType() + " " + event.schemaVersion(), false);
            return null;
        }
        if (inbox.existsById(event.eventId())) {
            return resumeDuplicateEvent(event);
        }

        LoanApplicationSubmittedPayload payload =
                json.fromJson(event.payload().toString(), LoanApplicationSubmittedPayload.class);
        validateEnvelope(event, payload);
        String payloadHash = json.sha256(event.payload().toString());
        var existingCase = decisionCases.findByApplicationId(payload.applicationId());
        if (existingCase.isPresent()) {
            DecisionCaseEntity existing = existingCase.get();
            if (!existing.getSourcePayloadHash().equals(payloadHash)) {
                existing.markRecalculationRequired("CONFLICTING_SUBMITTED_EVENT", clock.instant());
            }
            recordInbox(event, payloadHash);
            return AgentExecution.skipped();
        }

        Instant now = databaseTimestamp(clock.instant());
        String caseId = "case-" + payload.applicationId();
        DecisionCaseEntity decisionCase = decisionCases.saveAndFlush(new DecisionCaseEntity(
                caseId,
                payload.applicationId(),
                payload.applicantId(),
                payload.inputSnapshotVersion(),
                payloadHash,
                now
        ));
        outbox.save(reviewStartedEvent(decisionCase, event, now));
        if (payload.inputSnapshotVersion() == null || payload.inputSnapshotVersion().isBlank()) {
            decisionCase.markVerificationRequired("SNAPSHOT_REFERENCE_MISSING",
                    AssuranceResult.ASSURANCE_INCOMPLETE.name(), now);
            recordInbox(event, payloadHash);
            return AgentExecution.skipped();
        }
        if (snapshot == null || !snapshot.available()) {
            String classification = snapshot == null ? "VALIDATION_REQUIRED" : snapshot.classification();
            String reasonCode = snapshot == null ? "FEATURE_SNAPSHOT_UNAVAILABLE" : snapshot.reasonCode();
            if ("BLOCKED".equals(classification)) {
                decisionCase.markBlocked(reasonCode, "FEATURE_SNAPSHOT_REJECTED", now);
            } else {
                decisionCase.markVerificationRequired(reasonCode, "FEATURE_SNAPSHOT_REJECTED", now);
            }
            log.info("Phase 2 feature snapshot rejected applicationId={} caseId={} classification={} reasonCode={}",
                    payload.applicationId(), decisionCase.getCaseId(), classification, reasonCode);
            recordInbox(event, payloadHash);
            return AgentExecution.skipped();
        }

        decisionCase.markPreflightCompleted(now);
        decisionCase.transitionTo(DecisionCaseStatus.EVALUATION_REQUESTED, now);
        EvaluationRunEntity run = createEvaluationRun(decisionCase, payload, event, snapshot.snapshot(), now);
        ObjectNode request = buildAgentRequest(decisionCase, run);
        contracts.validate(REQUEST_SCHEMA, request);
        run.start();
        OutboxEventEntity requestedEvent = agentEvaluationRequestedEvent(decisionCase, run, event.eventId(), now.plusMillis(1));
        outbox.save(requestedEvent);
        run.attachRequestEvent(requestedEvent.getEventId());
        recordInbox(event, payloadHash);
        log.info("Phase 2 agent execution planned applicationId={} caseId={} evaluationRunId={} agentRunId={}",
                payload.applicationId(), caseId, run.getEvaluationRunId(), run.getAgentRunId());
        return new AgentExecution(event, decisionCase.getCaseId(), run.getEvaluationRunId(), run.getAgentRunId(), request, false);
    }

    private EvaluationRunEntity createEvaluationRun(DecisionCaseEntity decisionCase,
                                                    LoanApplicationSubmittedPayload payload,
                                                    EventEnvelope cause,
                                                    Phase2FeatureSnapshot snapshot,
                                                    Instant now) {
        UUID evaluationRunId = UUID.randomUUID();
        UUID agentRunId = UUID.randomUUID();
        Instant requestedAt = databaseTimestamp(now);
        Instant deadline = databaseTimestamp(requestedAt.plus(executionPlan.requestTimeout()));
        String requestIdempotencyKey = requestIdempotencyKey(
                decisionCase.getCaseId(),
                evaluationRunId,
                snapshot.snapshotVersion(),
                snapshot.snapshotReference().path("snapshotDigest").asText(),
                snapshot.featurePayloadDigest());
        EvaluationRunEntity run = new EvaluationRunEntity(
                evaluationRunId,
                decisionCase,
                executionPlan.planVersion(),
                snapshot.snapshotVersion(),
                UUID.randomUUID(),
                json.canonicalJson(componentVersions()),
                requestedAt
        );
        run.configurePhase2(
                executionPlan.planVersion(),
                agentRunId,
                requestIdempotencyKey,
                snapshot.snapshotId().toString(),
                snapshot.snapshotSchemaVersion(),
                snapshot.snapshotReference().path("snapshotDigest").asText(),
                cause.eventId(),
                databaseTimestamp(snapshot.createdAt()),
                snapshot.snapshotReference().path("snapshotReference").asText(),
                snapshot.snapshotReference().path("referenceType").asText(),
                snapshot.featureSchemaVersion(),
                snapshot.featurePayloadDigest(),
                json.canonicalJson(snapshot.featurePayload()),
                executionPlan.preprocessingVersion(),
                executionPlan.modelVersion(),
                executionPlan.modelArtifactDigest(),
                executionPlan.thresholdVersion(),
                executionPlan.maxAttempts(),
                requestedAt,
                deadline
        );
        return evaluationRuns.saveAndFlush(run);
    }

    private void executeAgentRun(AgentExecution execution) {
        AttemptLease attempt;
        try {
            attempt = transactions.execute(status -> claimNextAttempt(execution));
        } catch (OptimisticLockingFailureException conflict) {
            log.debug("Run already claimed evaluationRunId={}", execution.evaluationRunId());
            return;
        }
        if (attempt == null) {
            return;
        }
        if (!clock.instant().isBefore(Instant.parse(execution.request().get("deadlineAt").asText()))) {
            transactions.executeWithoutResult(status -> recordTransportFailure(
                    execution, attempt.attemptId(), "AGENT_TIMEOUT", true));
            return;
        }

        try {
            JsonNode result = agentClient.execute(execution.request());
            try {
                contracts.validate(RESULT_SCHEMA, result);
            } catch (ContractValidationException exception) {
                transactions.executeWithoutResult(status ->
                        recordContractInvalidResult(execution, attempt.attemptId(), result));
                return;
            }
            int attemptId = result.path("agentRun").path("attemptId").asInt(attempt.attemptId());
            boolean retryableIdentityAccepted = Boolean.TRUE.equals(transactions.execute(status ->
                    retryableResultIdentityAccepted(execution, result, attempt.attemptId())));
            if (!retryableIdentityAccepted) {
                transactions.executeWithoutResult(status -> validateAndRecordResult(execution, result, attempt.attemptId()));
                return;
            }
            if (isRetryableFailed(result) && attemptId < executionPlan.maxAttempts()) {
                transactions.executeWithoutResult(status -> recordAgentRetryableFailure(execution, attemptId, result));
                return;
            }
            transactions.executeWithoutResult(status -> validateAndRecordResult(execution, result, attempt.attemptId()));
        } catch (AgentRuntimeTimeoutException exception) {
            transactions.executeWithoutResult(status -> recordTransportFailure(
                    execution, attempt.attemptId(), "AGENT_TIMEOUT", false));
        } catch (AgentRuntimeTransportException exception) {
            transactions.executeWithoutResult(status -> recordTransportFailure(
                    execution, attempt.attemptId(), "AGENT_RUNTIME_TEMPORARY_FAILURE", false));
        } catch (OptimisticLockingFailureException conflict) {
            log.info("Skipped stale Phase 2 agent result evaluationRunId={}", execution.evaluationRunId());
        }
    }

    private void recordContractInvalidResult(AgentExecution execution, int claimedAttemptId, JsonNode malformedResult) {
        EvaluationRunEntity run = evaluationRuns.findById(execution.evaluationRunId()).orElseThrow();
        DecisionCaseEntity decisionCase = decisionCases.findById(execution.caseId()).orElseThrow();
        if (run.getStatus() != EvaluationRunStatus.RUNNING) {
            log.info("Ignored malformed stale Phase 2 agent result evaluationRunId={} status={}",
                    run.getEvaluationRunId(), run.getStatus());
            return;
        }
        run.recordAttempt(claimedAttemptId);
        String resultDigest = json.sha256Prefixed(json.canonicalJson(malformedResult));
        Instant now = clock.instant();
        run.rejectPhase2("VALIDATION_REQUIRED", "CONTRACT_VALIDATION_FAILED", resultDigest, now);
        decisionCase.markVerificationRequired("CONTRACT_VALIDATION_FAILED", "AGENT_RESULT_REJECTED", now);
        outbox.save(agentResultValidatedEvent(decisionCase, run, claimedAttemptId, resultDigest,
                "REJECTED", List.of("SCHEMA_INVALID"), now.plusMillis(1)));
    }

    private void validateAndRecordResult(AgentExecution execution, JsonNode result, int claimedAttemptId) {
        EvaluationRunEntity run = evaluationRuns.findById(execution.evaluationRunId()).orElseThrow();
        DecisionCaseEntity decisionCase = decisionCases.findById(execution.caseId()).orElseThrow();
        int attemptId = result.path("agentRun").path("attemptId").asInt(1);
        String resultDigest = json.sha256Prefixed(json.canonicalJson(result));
        if (run.getAcceptedResultDigest() != null) {
            if (run.getAcceptedResultDigest().equals(resultDigest)) {
                log.info("Ignored duplicate Phase 2 agent result evaluationRunId={} digest={}",
                        run.getEvaluationRunId(), resultDigest);
                return;
            }
            Instant conflictAt = clock.instant();
            run.blockResultConflict("AGENT_RUN_RESULT_CONFLICT", conflictAt);
            decisionCase.markBlocked("AGENT_RUN_RESULT_CONFLICT", "AGENT_RESULT_CONFLICT", conflictAt);
            return;
        }
        if (run.getStatus() != EvaluationRunStatus.RUNNING) {
            log.info("Ignored stale Phase 2 agent result evaluationRunId={} status={}",
                    run.getEvaluationRunId(), run.getStatus());
            return;
        }
        boolean completed = "COMPLETED".equals(result.path("resultStatus").asText());
        Instant completedAt = parseInstant(result.path("completedAt").asText());
        if (!isAcceptedRuntimeAttempt(attemptId, claimedAttemptId)) {
            run.rejectPhase2("BLOCKED", "AGENT_ATTEMPT_MISMATCH", resultDigest, completedAt);
            decisionCase.markBlocked("AGENT_ATTEMPT_MISMATCH", "AGENT_RESULT_REJECTED", completedAt);
            return;
        }
        run.recordAttempt(attemptId);
        if (completed && completedAt.isAfter(run.getDeadlineAt())) {
            run.rejectPhase2("VALIDATION_REQUIRED", "DEADLINE_EXCEEDED", resultDigest, completedAt);
            decisionCase.markVerificationRequired("DEADLINE_EXCEEDED", "AGENT_RESULT_REJECTED", completedAt);
            return;
        }
        List<String> reasonCodes = validateResultSemantics(run, result);
        if (decisionCase.getStatus() == DecisionCaseStatus.RECALCULATION_REQUIRED) {
            run.failOrchestration("VALIDATION_REQUIRED", "SUPERSEDED_BY_CONFLICTING_INPUT", completedAt);
            return;
        }
        if (completed && reasonCodes.equals(validatedReasonCodes())) {
            run.completePhase2(resultDigest, json.canonicalJson(result.get("proposal")),
                    json.canonicalJson(result.get("proposal").get("reasonCodes")), completedAt);
            decisionCase.completeEvaluation(completedAt);
            Instant validatedAt = completedAt.plusMillis(1);
            outbox.save(agentResultValidatedEvent(decisionCase, run, attemptId, resultDigest,
                    "VALIDATED", reasonCodes, validatedAt));
            return;
        }

        String classification = failureClassification(result, reasonCodes);
        String reasonCode = failureReasonCode(result, reasonCodes);
        run.rejectPhase2(classification, reasonCode, resultDigest, completedAt);
        if ("BLOCKED".equals(classification)) {
            decisionCase.markBlocked(reasonCode, "AGENT_RESULT_REJECTED", completedAt);
        } else {
            decisionCase.markVerificationRequired(reasonCode, "AGENT_RESULT_REJECTED", completedAt);
        }
        log.info("Phase 2 agent result rejected evaluationRunId={} classification={} reasonCode={} reasonCodes={}",
                run.getEvaluationRunId(), classification, reasonCode, reasonCodes);
        Instant validatedAt = completedAt.plusMillis(1);
        outbox.save(agentResultValidatedEvent(decisionCase, run, attemptId, resultDigest,
                "REJECTED", reasonCodes, validatedAt));
    }

    private AttemptLease claimNextAttempt(AgentExecution execution) {
        EvaluationRunEntity run = evaluationRuns.findById(execution.evaluationRunId()).orElseThrow();
        if (run.getStatus() != EvaluationRunStatus.RUNNING) {
            return null;
        }
        Instant now = clock.instant();
        if (run.getLeaseUntil() != null && run.getLeaseUntil().isAfter(now)) {
            return null;
        }
        if (run.getNextAttemptAt() != null && run.getNextAttemptAt().isAfter(now)) {
            return null;
        }
        if (run.getAttemptCount() >= run.getMaxAttempts()) {
            DecisionCaseEntity decisionCase = decisionCases.findById(execution.caseId()).orElseThrow();
            run.failOrchestration("RETRYABLE", "RETRY_EXHAUSTED", now);
            decisionCase.markVerificationRequired("RETRY_EXHAUSTED", "AGENT_RUNTIME_RETRY_EXHAUSTED", now);
            return null;
        }
        int attemptId = run.getAttemptCount() + 1;
        run.recordAttemptStarted(attemptId, now, instanceId, now.plus(executionPlan.leaseDuration()));
        return new AttemptLease(attemptId);
    }

    private void recordAgentRetryableFailure(AgentExecution execution, int attemptId, JsonNode result) {
        EvaluationRunEntity run = evaluationRuns.findById(execution.evaluationRunId()).orElseThrow();
        run.recordAttempt(attemptId);
        Instant now = clock.instant();
        run.recordRetryableTransportFailure(
                result.path("failure").path("reasonCode").asText("AGENT_RETRYABLE_FAILURE"),
                now,
                nextAttemptAt(attemptId, now)
        );
    }

    private void recordTransportFailure(AgentExecution execution, int attemptId, String reasonCode, boolean deadlineExpired) {
        EvaluationRunEntity run = evaluationRuns.findById(execution.evaluationRunId()).orElseThrow();
        DecisionCaseEntity decisionCase = decisionCases.findById(execution.caseId()).orElseThrow();
        run.recordAttempt(attemptId);
        Instant now = clock.instant();
        boolean exhausted = deadlineExpired
                || attemptId >= run.getMaxAttempts()
                || !nextAttemptAt(attemptId, now).isBefore(run.getDeadlineAt());
        if (exhausted) {
            run.failOrchestration("RETRYABLE", reasonCode, now);
            decisionCase.markVerificationRequired(reasonCode, "AGENT_RUNTIME_TRANSPORT_FAILURE", now);
            return;
        }
        run.recordRetryableTransportFailure(reasonCode, now, nextAttemptAt(attemptId, now));
    }

    private Instant nextAttemptAt(int attemptId, Instant now) {
        long multiplier = Math.max(1, Math.min(8, 1L << Math.min(3, Math.max(0, attemptId - 1))));
        return now.plus(executionPlan.retryBackoff().multipliedBy(multiplier));
    }

    private List<String> validateResultSemantics(EvaluationRunEntity run, JsonNode result) {
        List<String> reasons = new ArrayList<>();
        reasons.add("SCHEMA_VALID");
        if (matchesText(result, "featureSchemaVersion", run.getFeatureSchemaVersion())
                && matchesText(result, "preprocessingVersion", run.getPreprocessingVersion())
                && matchesText(result, "modelVersion", run.getModelVersion())
                && matchesText(result, "modelArtifactDigest", run.getModelArtifactDigest())
                && matchesText(result, "thresholdVersion", run.getThresholdVersion())) {
            reasons.add("MODEL_PROVENANCE_VALID");
        } else {
            reasons.add("MODEL_PROVENANCE_INVALID");
        }
        JsonNode agentRun = result.get("agentRun");
        if (agentRun == null
                || !run.getDecisionCase().getCaseId().equals(agentRun.path("decisionCaseId").asText())
                || !run.getEvaluationRunId().toString().equals(agentRun.path("evaluationRunId").asText())
                || !run.getAgentRunId().toString().equals(agentRun.path("agentRunId").asText())
                || !run.getRequestIdempotencyKey().equals(agentRun.path("requestIdempotencyKey").asText())) {
            reasons.add("MODEL_PROVENANCE_INVALID");
        }
        if (snapshotIdentityMatches(run, result.path("snapshotReference"))) {
            reasons.add("SNAPSHOT_MATCHED");
        } else {
            reasons.add("SNAPSHOT_MISMATCH");
        }
        if ("COMPLETED".equals(result.path("resultStatus").asText())
                && result.hasNonNull("explanationRef")
                && result.hasNonNull("explanationDigest")
                && result.hasNonNull("proposal")) {
            reasons.add("SHAP_PRESENT");
        } else if ("COMPLETED".equals(result.path("resultStatus").asText())) {
            reasons.add("SHAP_MISSING");
        }
        if ("FAILED".equals(result.path("resultStatus").asText())) {
            reasons.add("AGENT_FAILURE_RECORDED");
        }
        return reasons.stream().distinct().toList();
    }

    private boolean isAcceptedRuntimeAttempt(int resultAttemptId, int claimedAttemptId) {
        return resultAttemptId >= 1 && resultAttemptId <= claimedAttemptId;
    }

    private boolean snapshotIdentityMatches(EvaluationRunEntity run, JsonNode snapshot) {
        return matchesText(snapshot, "snapshotId", run.getSnapshotId())
                && matchesText(snapshot, "snapshotVersion", run.getInputSnapshotVersion())
                && matchesText(snapshot, "snapshotSchemaVersion", run.getSnapshotSchemaVersion())
                && matchesInstant(snapshot, "snapshotCreatedAt", run.getSnapshotCreatedAt())
                && matchesText(snapshot, "snapshotDigest", run.getSnapshotDigest())
                && matchesText(snapshot, "snapshotReference", run.getSnapshotReference())
                && matchesText(snapshot, "referenceType", run.getReferenceType());
    }

    private ObjectNode buildAgentRequest(DecisionCaseEntity decisionCase, EvaluationRunEntity run) {
        Map<String, Object> snapshotReference = new LinkedHashMap<>();
        snapshotReference.put("schemaVersion", "1.0.0");
        snapshotReference.put("snapshotId", run.getSnapshotId());
        snapshotReference.put("snapshotVersion", run.getInputSnapshotVersion());
        snapshotReference.put("snapshotSchemaVersion", run.getSnapshotSchemaVersion());
        snapshotReference.put("snapshotCreatedAt", run.getSnapshotCreatedAt().toString());
        snapshotReference.put("digestAlgorithm", "sha256");
        snapshotReference.put("snapshotDigest", run.getSnapshotDigest());
        snapshotReference.put("snapshotReference", run.getSnapshotReference());
        snapshotReference.put("referenceType", run.getReferenceType());

        Map<String, Object> request = new LinkedHashMap<>();
        request.put("schemaVersion", "1.0.0");
        request.put("decisionCaseId", decisionCase.getCaseId());
        request.put("evaluationRunId", run.getEvaluationRunId().toString());
        request.put("agentRunId", run.getAgentRunId().toString());
        request.put("agentType", "LOAN_DECISION_AGENT");
        request.put("requestIdempotencyKey", run.getRequestIdempotencyKey());
        request.put("snapshotReference", snapshotReference);
        request.put("featureSchemaVersion", run.getFeatureSchemaVersion());
        request.put("featurePayload", json.fromJson(run.getFeaturePayload(), Map.class));
        request.put("preprocessingVersion", run.getPreprocessingVersion());
        request.put("modelVersion", run.getModelVersion());
        request.put("modelArtifactDigest", run.getModelArtifactDigest());
        request.put("thresholdVersion", run.getThresholdVersion());
        request.put("requestedAt", run.getRequestedAt().toString());
        request.put("deadlineAt", run.getDeadlineAt().toString());
        request.put("correlationId", decisionCase.getApplicationId().toString());
        request.put("causationId", run.getSourceEventId().toString());
        return json.toJsonNode(request).deepCopy();
    }

    private boolean isRetryableFailed(JsonNode result) {
        return "FAILED".equals(result.path("resultStatus").asText())
                && "RETRYABLE".equals(result.path("failure").path("classification").asText());
    }

    private boolean retryableResultIdentityAccepted(AgentExecution execution, JsonNode result, int claimedAttemptId) {
        if (!isRetryableFailed(result)) {
            return true;
        }
        EvaluationRunEntity run = evaluationRuns.findById(execution.evaluationRunId()).orElseThrow();
        int attemptId = result.path("agentRun").path("attemptId").asInt(claimedAttemptId);
        List<String> reasonCodes = validateResultSemantics(run, result);
        return isAcceptedRuntimeAttempt(attemptId, claimedAttemptId)
                && reasonCodes.contains("MODEL_PROVENANCE_VALID")
                && reasonCodes.contains("SNAPSHOT_MATCHED");
    }

    private String requestIdempotencyKey(String decisionCaseId, UUID evaluationRunId, String snapshotVersion,
                                         String snapshotDigest, String featurePayloadDigest) {
        Map<String, Object> keyInputs = new LinkedHashMap<>();
        keyInputs.put("decisionCaseId", decisionCaseId);
        keyInputs.put("evaluationRunId", evaluationRunId.toString());
        keyInputs.put("agentType", "LOAN_DECISION_AGENT");
        keyInputs.put("snapshotVersion", snapshotVersion);
        keyInputs.put("snapshotDigest", snapshotDigest);
        keyInputs.put("featurePayloadDigest", featurePayloadDigest);
        keyInputs.put("featureSchemaVersion", executionPlan.featureSchemaVersion());
        keyInputs.put("preprocessingVersion", executionPlan.preprocessingVersion());
        keyInputs.put("modelVersion", executionPlan.modelVersion());
        keyInputs.put("modelArtifactDigest", executionPlan.modelArtifactDigest());
        keyInputs.put("thresholdVersion", executionPlan.thresholdVersion());
        return "sha256:" + json.sha256(json.canonicalJson(keyInputs));
    }

    private List<String> validatedReasonCodes() {
        return List.of("SCHEMA_VALID", "MODEL_PROVENANCE_VALID", "SNAPSHOT_MATCHED", "SHAP_PRESENT");
    }

    private boolean matchesText(JsonNode result, String field, String expected) {
        return expected != null && expected.equals(result.path(field).asText(null));
    }

    private boolean matchesInstant(JsonNode result, String field, Instant expected) {
        String value = result.path(field).asText(null);
        return expected != null && value != null && expected.equals(parseInstant(value));
    }

    private String failureClassification(JsonNode result, List<String> reasonCodes) {
        if (reasonCodes.contains("MODEL_PROVENANCE_INVALID") || reasonCodes.contains("SNAPSHOT_MISMATCH")) {
            return "BLOCKED";
        }
        return result.path("failure").path("classification").asText("VALIDATION_REQUIRED");
    }

    private String failureReasonCode(JsonNode result, List<String> reasonCodes) {
        if (reasonCodes.contains("MODEL_PROVENANCE_INVALID")) {
            return "MODEL_ARTIFACT_DIGEST_MISMATCH";
        }
        if (reasonCodes.contains("SNAPSHOT_MISMATCH")) {
            return "SNAPSHOT_IDENTITY_MISMATCH";
        }
        if (reasonCodes.contains("SHAP_MISSING")) {
            return "SHAP_CALCULATION_FAILED";
        }
        return result.path("failure").path("reasonCode").asText("CONTRACT_VALIDATION_FAILED");
    }

    private Instant parseInstant(String value) {
        return OffsetDateTime.parse(value).toInstant();
    }

    private Instant databaseTimestamp(Instant value) {
        return value == null ? null : value.truncatedTo(ChronoUnit.MICROS);
    }

    private AgentExecution recoverConcurrentDecisionCaseExecution(EventEnvelope event) {
        return transactions.execute(status -> {
            if (!supportsEvent(event, "loan.application.submitted.v1", LOAN_SUBMITTED_SCHEMA_VERSION)) {
                return null;
            }
            if (inbox.existsById(event.eventId())) {
                return resumeDuplicateEvent(event);
            }
            LoanApplicationSubmittedPayload payload =
                    json.fromJson(event.payload().toString(), LoanApplicationSubmittedPayload.class);
            validateEnvelope(event, payload);
            String payloadHash = json.sha256(event.payload().toString());
            return decisionCases.findByApplicationId(payload.applicationId())
                    .map(existing -> {
                        if (!existing.getSourcePayloadHash().equals(payloadHash)) {
                            existing.markRecalculationRequired("CONCURRENT_CONFLICTING_SUBMITTED_EVENT", clock.instant());
                        }
                        recordInbox(event, payloadHash);
                        return AgentExecution.skipped();
                    })
                    .orElse(null);
        });
    }

    private AgentExecution recoverPlanningConflict(EventEnvelope event, RuntimeException original) {
        RuntimeException latest = original;
        for (int attempt = 0; attempt < executionPlan.planningRecoveryAttempts(); attempt++) {
            try {
                AgentExecution execution = recoverConcurrentDecisionCaseExecution(event);
                if (execution != null) {
                    return execution;
                }
            } catch (DataIntegrityViolationException | OptimisticLockingFailureException retryable) {
                latest = retryable;
            }
            backoffPlanningRecovery(attempt);
        }
        throw latest;
    }

    private void backoffPlanningRecovery(int attempt) {
        if (attempt >= executionPlan.planningRecoveryAttempts() - 1 || executionPlan.planningRecoveryBackoff().isZero()) {
            return;
        }
        LockSupport.parkNanos(executionPlan.planningRecoveryBackoff().multipliedBy(attempt + 1L).toNanos());
    }

    private AgentExecution resumeDuplicateEvent(EventEnvelope event) {
        LoanApplicationSubmittedPayload payload =
                json.fromJson(event.payload().toString(), LoanApplicationSubmittedPayload.class);
        validateEnvelope(event, payload);
        String payloadHash = json.sha256(event.payload().toString());
        return decisionCases.findByApplicationId(payload.applicationId())
                .flatMap(decisionCase -> {
                    if (!decisionCase.getSourcePayloadHash().equals(payloadHash)) {
                        decisionCase.markRecalculationRequired("DUPLICATE_EVENT_PAYLOAD_CONFLICT", clock.instant());
                        return java.util.Optional.<AgentExecution>empty();
                    }
                    return evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(decisionCase.getCaseId())
                            .filter(run -> run.getStatus() == EvaluationRunStatus.RUNNING)
                            .map(run -> executionFromRun(run));
                })
                .orElseGet(AgentExecution::skipped);
    }

    private AgentExecution executionFromRun(EvaluationRunEntity run) {
        DecisionCaseEntity decisionCase = run.getDecisionCase();
        ObjectNode request = buildAgentRequest(decisionCase, run);
        contracts.validate(REQUEST_SCHEMA, request);
        return new AgentExecution(null, decisionCase.getCaseId(), run.getEvaluationRunId(), run.getAgentRunId(), request, false);
    }

    @Transactional
    public void quarantineMalformedEvent(String rawMessage, String failureMessage) {
        quarantine.save(new GovernanceEventQuarantineEntity(
                UUID.randomUUID(),
                null,
                null,
                null,
                json.sha256(rawMessage),
                QuarantineFailureCode.MALFORMED_EVENT,
                failureMessage,
                clock.instant(),
                false
        ));
    }

    @Transactional(readOnly = true)
    public DecisionCaseResponse get(String caseId) {
        DecisionCaseEntity decisionCase = decisionCases.findById(caseId)
                .orElseThrow(() -> new NotFoundException("Decision case not found: " + caseId));
        return toResponse(decisionCase);
    }

    @Transactional(readOnly = true)
    public DecisionCaseResponse getByApplication(UUID applicationId) {
        DecisionCaseEntity decisionCase = decisionCases.findByApplicationId(applicationId)
                .orElseThrow(() -> new NotFoundException("Decision case not found for application: " + applicationId));
        return toResponse(decisionCase);
    }

    private void validateEnvelope(EventEnvelope event, LoanApplicationSubmittedPayload payload) {
        if (!"loan-service".equals(event.producer())) {
            throw new IllegalArgumentException("Submitted event producer must be loan-service");
        }
        if (!event.applicationId().equals(payload.applicationId())) {
            throw new IllegalArgumentException("Envelope and payload applicationId differ");
        }
        if (!event.applicationId().toString().equals(event.correlationId())) {
            throw new IllegalArgumentException("Phase 1 correlationId must equal applicationId");
        }
        if (payload.applicantId() == null || payload.applicantId().isBlank()) {
            throw new IllegalArgumentException("Submitted event requires applicantId");
        }
    }

    private boolean supportsEvent(EventEnvelope event, String eventType, String schemaVersion) {
        return eventType.equals(event.eventType()) && schemaVersion.equals(event.schemaVersion());
    }

    private void recordInbox(EventEnvelope event, String payloadHash) {
        try {
            inbox.save(new InboxEventEntity(
                    event.eventId(),
                    event.eventType(),
                    event.applicationId(),
                    payloadHash,
                    clock.instant()
            ));
        } catch (DataIntegrityViolationException ignoredDuplicate) {
            log.info("Duplicate inbox event ignored eventId={}", event.eventId());
        }
    }

    private void quarantine(EventEnvelope event, QuarantineFailureCode failureCode, String failureMessage, boolean retryable) {
        quarantine.save(new GovernanceEventQuarantineEntity(
                UUID.randomUUID(),
                event.eventId(),
                event.eventType(),
                event.schemaVersion(),
                json.sha256(event.payload() == null ? "" : event.payload().toString()),
                failureCode,
                failureMessage,
                clock.instant(),
                retryable
        ));
    }

    private OutboxEventEntity reviewStartedEvent(DecisionCaseEntity decisionCase, EventEnvelope cause, Instant now) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("applicationId", decisionCase.getApplicationId().toString());
        payload.put("reviewStartedAt", now.toString());
        return event("governance.review.started.v1", LOAN_SUBMITTED_SCHEMA_VERSION, decisionCase, null, cause.eventId(), payload, now);
    }

    private OutboxEventEntity agentEvaluationRequestedEvent(DecisionCaseEntity decisionCase, EvaluationRunEntity run,
                                                            UUID causeEventId, Instant now) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("evaluationRunId", run.getEvaluationRunId().toString());
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("inputSnapshotVersion", run.getInputSnapshotVersion());
        payload.put("executionPlanVersion", run.getExecutionPlanVersion());
        payload.put("evaluationMode", "AGENT");
        OutboxEventEntity event = event("agent.evaluation.requested.v1", REQUESTED_EVENT_SCHEMA_VERSION,
                decisionCase, run.getEvaluationRunId(), causeEventId, payload, now);
        contracts.validate(REQUESTED_EVENT_SCHEMA, json.toJsonNode(json.fromJson(event.getPayload(), Map.class)));
        return event;
    }

    private OutboxEventEntity agentResultValidatedEvent(DecisionCaseEntity decisionCase, EvaluationRunEntity run,
                                                        int attemptId, String resultDigest, String outcome,
                                                        List<String> reasonCodes, Instant now) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("evaluationRunId", run.getEvaluationRunId().toString());
        payload.put("agentRunId", run.getAgentRunId().toString());
        payload.put("attemptId", attemptId);
        payload.put("agentResultReference", "agent-result://" + decisionCase.getCaseId() + "/" + run.getAgentRunId() + "/attempt-" + attemptId);
        payload.put("agentResultDigest", resultDigest);
        payload.put("validationOutcome", outcome);
        payload.put("validationReasonCodes", reasonCodes);
        payload.put("validatedSchemaVersion", "1.0.0");
        payload.put("validatedAt", now.toString());
        OutboxEventEntity event = event("governance.agent-result.validated.v1", GOVERNANCE_AGENT_EVENT_SCHEMA_VERSION,
                decisionCase, run.getEvaluationRunId(), run.getRequestEventId(), payload, now);
        contracts.validate(VALIDATED_EVENT_SCHEMA, json.toJsonNode(json.fromJson(event.getPayload(), Map.class)));
        return event;
    }

    private OutboxEventEntity event(String eventType, String schemaVersion, DecisionCaseEntity decisionCase,
                                    UUID evaluationRunId, UUID causationId, Map<String, Object> payload, Instant now) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", UUID.randomUUID().toString());
        envelope.put("eventType", eventType);
        envelope.put("schemaVersion", schemaVersion);
        envelope.put("occurredAt", now.toString());
        envelope.put("producer", "governance-service");
        envelope.put("applicationId", decisionCase.getApplicationId().toString());
        envelope.put("caseId", decisionCase.getCaseId());
        envelope.put("evaluationRunId", evaluationRunId == null ? null : evaluationRunId.toString());
        envelope.put("correlationId", decisionCase.getApplicationId().toString());
        envelope.put("causationId", causationId == null ? null : causationId.toString());
        envelope.put("payload", payload);

        return new OutboxEventEntity(
                UUID.fromString((String) envelope.get("eventId")),
                eventType,
                schemaVersion,
                decisionCase.getApplicationId(),
                decisionCase.getApplicationId().toString(),
                causationId,
                json.canonicalJson(envelope),
                now
        );
    }

    private DecisionCaseResponse toResponse(DecisionCaseEntity decisionCase) {
        EvaluationRunEntity run = evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(decisionCase.getCaseId())
                .orElse(null);
        return new DecisionCaseResponse(
                "1.0.0",
                decisionCase.getCaseId(),
                decisionCase.getApplicationId(),
                decisionCase.getStatus(),
                decisionCase.getInputSnapshotVersion(),
                run == null ? null : run.getEvaluationRunId(),
                run == null ? null : run.getStatus(),
                run == null ? null : run.getProposal(),
                decisionCase.getFinalDecision(),
                decisionCase.getAssuranceResult(),
                decisionCase.getCreatedAt(),
                decisionCase.getUpdatedAt()
        );
    }

    private List<Map<String, String>> componentVersions() {
        return List.of(
                Map.of("componentType", "MODEL", "componentName", "loan-decision-model", "version", executionPlan.modelVersion()),
                Map.of("componentType", "PREPROCESSING", "componentName", "loan-feature-preprocessing", "version", executionPlan.preprocessingVersion()),
                Map.of("componentType", "THRESHOLD", "componentName", "loan-threshold", "version", executionPlan.thresholdVersion()),
                Map.of("componentType", "AGENT", "componentName", "loan-decision-agent", "version", "phase2-runtime")
        );
    }

    private record AgentExecution(
            EventEnvelope cause,
            String caseId,
            UUID evaluationRunId,
            UUID agentRunId,
            JsonNode request,
            boolean alreadyProcessed
    ) {
        static AgentExecution skipped() {
            return new AgentExecution(null, null, null, null, null, true);
        }
    }

    private record AttemptLease(int attemptId) {
    }

    private record SnapshotAcquisition(
            Phase2FeatureSnapshot snapshot,
            String classification,
            String reasonCode,
            boolean needed
    ) {
        static SnapshotAcquisition available(Phase2FeatureSnapshot snapshot) {
            return new SnapshotAcquisition(snapshot, null, null, true);
        }

        static SnapshotAcquisition failed(String classification, String reasonCode) {
            return new SnapshotAcquisition(null, classification, reasonCode, true);
        }

        static SnapshotAcquisition notNeeded() {
            return new SnapshotAcquisition(null, null, null, false);
        }

        boolean available() {
            return needed && snapshot != null;
        }
    }
}
