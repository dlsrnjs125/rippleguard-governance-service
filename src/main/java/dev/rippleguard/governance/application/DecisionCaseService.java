package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.AssuranceResult;
import dev.rippleguard.governance.domain.FinalDecision;
import dev.rippleguard.governance.domain.QuarantineFailureCode;
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
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

@Service
public class DecisionCaseService {
    private static final Logger log = LoggerFactory.getLogger(DecisionCaseService.class);
    private static final String EVENT_SCHEMA_VERSION = "1.1.0";

    private final DecisionCaseRepository decisionCases;
    private final EvaluationRunRepository evaluationRuns;
    private final InboxEventRepository inbox;
    private final OutboxEventRepository outbox;
    private final GovernanceEventQuarantineRepository quarantine;
    private final JsonSupport json;
    private final MockDecisionEvaluator evaluator;
    private final MockAssuranceEvaluator assurance;
    private final Clock clock;
    private final TransactionTemplate transactions;

    public DecisionCaseService(DecisionCaseRepository decisionCases,
                               EvaluationRunRepository evaluationRuns,
                               InboxEventRepository inbox,
                               OutboxEventRepository outbox,
                               GovernanceEventQuarantineRepository quarantine,
                               JsonSupport json,
                               MockDecisionEvaluator evaluator,
                               MockAssuranceEvaluator assurance,
                               Clock clock,
                               TransactionTemplate transactions) {
        this.decisionCases = decisionCases;
        this.evaluationRuns = evaluationRuns;
        this.inbox = inbox;
        this.outbox = outbox;
        this.quarantine = quarantine;
        this.json = json;
        this.evaluator = evaluator;
        this.assurance = assurance;
        this.clock = clock;
        this.transactions = transactions;
    }

    public void handleLoanApplicationSubmitted(EventEnvelope event) {
        try {
            transactions.executeWithoutResult(status -> handleLoanApplicationSubmittedInTransaction(event));
        } catch (DataIntegrityViolationException conflict) {
            if (!recoverConcurrentDecisionCase(event)) {
                throw conflict;
            }
        }
    }

    private void handleLoanApplicationSubmittedInTransaction(EventEnvelope event) {
        if (!supportsEvent(event, "loan.application.submitted.v1", EVENT_SCHEMA_VERSION)) {
            quarantine(event, QuarantineFailureCode.UNSUPPORTED_SCHEMA_VERSION,
                    "Unsupported event contract: " + event.eventType() + " " + event.schemaVersion(), false);
            return;
        }
        if (inbox.existsById(event.eventId())) {
            return;
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
            return;
        }

        EventTimeline timeline = EventTimeline.startingAt(clock.instant());
        String caseId = "case-" + payload.applicationId();
        DecisionCaseEntity decisionCase = decisionCases.saveAndFlush(new DecisionCaseEntity(
                caseId,
                payload.applicationId(),
                payload.applicantId(),
                payload.inputSnapshotVersion(),
                payloadHash,
                timeline.reviewStartedAt()
        ));
        outbox.save(reviewStartedEvent(decisionCase, event, timeline.reviewStartedAt()));
        if (payload.inputSnapshotVersion() == null || payload.inputSnapshotVersion().isBlank()) {
            decisionCase.markVerificationRequired("SNAPSHOT_REFERENCE_MISSING",
                    AssuranceResult.ASSURANCE_INCOMPLETE.name(), timeline.reviewStartedAt());
            recordInbox(event, payloadHash);
            return;
        }

        decisionCase.markPreflightCompleted(timeline.reviewStartedAt());
        MockEvaluationResult result = evaluator.evaluate(payload.applicationId(), caseId, payload.inputSnapshotVersion());
        decisionCase.transitionTo(DecisionCaseStatus.EVALUATION_REQUESTED, timeline.evaluationRequestedAt());
        EvaluationRunEntity run = evaluationRuns.save(new EvaluationRunEntity(
                result.evaluationRunId(),
                decisionCase,
                MockDecisionEvaluator.RULE_VERSION,
                payload.inputSnapshotVersion(),
                result.decisionId(),
                json.canonicalJson(componentVersions()),
                timeline.evaluationRequestedAt()
        ));
        OutboxEventEntity requested = evaluationRequestedEvent(
                decisionCase, run, event.eventId(), timeline.evaluationRequestedAt());
        outbox.save(requested);

        run.start();
        run.complete(result.proposal(), result.confidence(), json.canonicalJson(result.reasonCodes()),
                timeline.evaluationCompletedAt());
        decisionCase.completeEvaluation(timeline.evaluationCompletedAt());
        OutboxEventEntity completed = evaluationCompletedEvent(
                decisionCase, run, requested.getEventId(), timeline.evaluationCompletedAt());
        outbox.save(completed);

        MockAssuranceResult assuranceResult = assurance.evaluate(payload, result);
        if (assuranceResult.result() == AssuranceResult.ASSURANCE_INCOMPLETE) {
            decisionCase.markVerificationRequired(
                    assuranceResult.reasonCode(), assuranceResult.result().name(), timeline.decisionCommandedAt());
            recordInbox(event, payloadHash);
            return;
        }
        if (assuranceResult.result() == AssuranceResult.ASSURANCE_VIOLATED) {
            decisionCase.markBlocked(
                    assuranceResult.reasonCode(), assuranceResult.result().name(), timeline.decisionCommandedAt());
            recordInbox(event, payloadHash);
            return;
        }

        decisionCase.commandDecision(result.proposal(), assuranceResult.result().name(), timeline.decisionCommandedAt());
        outbox.save(decisionCommandedEvent(
                decisionCase, run, result, assuranceResult, completed.getEventId(), timeline.decisionCommandedAt()));
        recordInbox(event, payloadHash);
        log.info("Decision case created applicationId={} caseId={} evaluationRunId={} finalDecision={}",
                payload.applicationId(), caseId, run.getEvaluationRunId(), result.proposal());
    }

    private boolean recoverConcurrentDecisionCase(EventEnvelope event) {
        return Boolean.TRUE.equals(transactions.execute(status -> {
            if (!supportsEvent(event, "loan.application.submitted.v1", EVENT_SCHEMA_VERSION)) {
                return false;
            }
            if (inbox.existsById(event.eventId())) {
                return true;
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
                        log.info("Recovered concurrent submitted event applicationId={} caseId={} eventId={}",
                                payload.applicationId(), existing.getCaseId(), event.eventId());
                        return true;
                    })
                    .orElse(false);
        }));
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

        return event("governance.review.started.v1", decisionCase, null, cause.eventId(), payload, now);
    }

    private OutboxEventEntity evaluationRequestedEvent(DecisionCaseEntity decisionCase, EvaluationRunEntity run,
                                                       UUID causationId, Instant now) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("evaluationRunId", run.getEvaluationRunId().toString());
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("inputSnapshotVersion", run.getInputSnapshotVersion());
        payload.put("executionPlanVersion", run.getExecutionPlanVersion());
        payload.put("evaluationMode", "MOCK");

        return event("agent.evaluation.requested.v1", decisionCase, run.getEvaluationRunId(), causationId, payload, now);
    }

    private OutboxEventEntity evaluationCompletedEvent(DecisionCaseEntity decisionCase, EvaluationRunEntity run,
                                                       UUID causationId, Instant now) {
        Map<String, Object> generator = new LinkedHashMap<>();
        generator.put("agentName", MockDecisionEvaluator.EVALUATOR_ID);
        generator.put("agentVersion", "mock-evaluator-v1");
        generator.put("modelName", "loan-decision-model");
        generator.put("modelVersion", "mock-v1");
        generator.put("promptName", "loan-decision-prompt");
        generator.put("promptVersion", "mock-prompt-v1");

        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("schemaVersion", "1.0.0");
        envelope.put("decisionId", run.getDecisionId().toString());
        envelope.put("evaluationRunId", run.getEvaluationRunId().toString());
        envelope.put("decisionCaseId", decisionCase.getCaseId());
        envelope.put("evaluatorId", MockDecisionEvaluator.EVALUATOR_ID);
        envelope.put("originalPurpose", "LOAN_ELIGIBILITY_ASSESSMENT");
        envelope.put("subjectType", "LOAN_APPLICATION");
        envelope.put("proposal", "PROPOSE_" + run.getProposal().name());
        envelope.put("status", "PROPOSED");
        envelope.put("confidence", run.getConfidence());
        envelope.put("usedEvidenceRefs", List.of("snapshot://" + decisionCase.getApplicationId() + "/" + run.getInputSnapshotVersion()));
        envelope.put("generatorRef", generator);
        envelope.put("validUntil", now.plusSeconds(86400).toString());

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("evaluationRunId", run.getEvaluationRunId().toString());
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("evaluationMode", "MOCK");
        payload.put("evaluatorId", MockDecisionEvaluator.EVALUATOR_ID);
        payload.put("decisionEnvelope", envelope);

        return event("agent.evaluation.completed.v1", decisionCase, run.getEvaluationRunId(), causationId, payload, now);
    }

    private OutboxEventEntity decisionCommandedEvent(DecisionCaseEntity decisionCase, EvaluationRunEntity run,
                                                     MockEvaluationResult result, MockAssuranceResult assuranceResult,
                                                     UUID causationId, Instant now) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("commandId", commandId(decisionCase.getCaseId(), run.getEvaluationRunId()).toString());
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("applicationId", decisionCase.getApplicationId().toString());
        payload.put("decisionId", run.getDecisionId().toString());
        payload.put("evaluationRunId", run.getEvaluationRunId().toString());
        payload.put("evaluationRunStatus", "COMPLETED");
        payload.put("finalDecision", result.proposal().name());
        payload.put("assuranceResult", assuranceResult.result().name());
        payload.put("reasonCodes", List.of(assuranceResult.reasonCode()));
        payload.put("issuedAt", now.toString());
        payload.put("idempotencyKey", "decision-command-" + decisionCase.getCaseId());

        return event("loan.decision.commanded.v1", decisionCase, run.getEvaluationRunId(), causationId, payload, now);
    }

    private OutboxEventEntity event(String eventType, DecisionCaseEntity decisionCase, UUID evaluationRunId,
                                    UUID causationId, Map<String, Object> payload, Instant now) {
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("eventId", UUID.randomUUID().toString());
        envelope.put("eventType", eventType);
        envelope.put("schemaVersion", EVENT_SCHEMA_VERSION);
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
                EVENT_SCHEMA_VERSION,
                decisionCase.getApplicationId(),
                decisionCase.getApplicationId().toString(),
                causationId,
                json.canonicalJson(envelope),
                now
        );
    }

    private UUID commandId(String caseId, UUID evaluationRunId) {
        return UUID.nameUUIDFromBytes(("command:" + caseId + ":" + evaluationRunId).getBytes(StandardCharsets.UTF_8));
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
                Map.of("componentType", "MODEL", "componentName", "mock-model", "version", "mock-v1"),
                Map.of("componentType", "PROMPT", "componentName", "no-prompt", "version", "phase1"),
                Map.of("componentType", "TOOL", "componentName", "deterministic-rule", "version", MockDecisionEvaluator.RULE_VERSION),
                Map.of("componentType", "AGENT", "componentName", MockDecisionEvaluator.EVALUATOR_ID, "version", "mock-evaluator-v1")
        );
    }

    private record EventTimeline(
            Instant reviewStartedAt,
            Instant evaluationRequestedAt,
            Instant evaluationCompletedAt,
            Instant decisionCommandedAt
    ) {
        static EventTimeline startingAt(Instant base) {
            return new EventTimeline(
                    base,
                    base.plusMillis(1),
                    base.plusMillis(2),
                    base.plusMillis(3)
            );
        }
    }
}
