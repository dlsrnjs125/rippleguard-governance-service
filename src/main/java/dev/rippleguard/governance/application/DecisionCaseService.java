package dev.rippleguard.governance.application;

import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.FinalDecision;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseEntity;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseRepository;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunEntity;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunRepository;
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

@Service
public class DecisionCaseService {
    private static final Logger log = LoggerFactory.getLogger(DecisionCaseService.class);
    private static final String EVENT_SCHEMA_VERSION = "1.1.0";

    private final DecisionCaseRepository decisionCases;
    private final EvaluationRunRepository evaluationRuns;
    private final InboxEventRepository inbox;
    private final OutboxEventRepository outbox;
    private final JsonSupport json;
    private final MockDecisionEvaluator evaluator;
    private final Clock clock;

    public DecisionCaseService(DecisionCaseRepository decisionCases,
                               EvaluationRunRepository evaluationRuns,
                               InboxEventRepository inbox,
                               OutboxEventRepository outbox,
                               JsonSupport json,
                               MockDecisionEvaluator evaluator,
                               Clock clock) {
        this.decisionCases = decisionCases;
        this.evaluationRuns = evaluationRuns;
        this.inbox = inbox;
        this.outbox = outbox;
        this.json = json;
        this.evaluator = evaluator;
        this.clock = clock;
    }

    @Transactional
    public void handleLoanApplicationSubmitted(EventEnvelope event) {
        requireEvent(event, "loan.application.submitted.v1", EVENT_SCHEMA_VERSION);
        if (inbox.existsById(event.eventId())) {
            return;
        }

        LoanApplicationSubmittedPayload payload =
                json.fromJson(event.payload().toString(), LoanApplicationSubmittedPayload.class);
        validateSubmitted(event, payload);
        if (decisionCases.findByApplicationId(payload.applicationId()).isPresent()) {
            recordInbox(event);
            return;
        }

        Instant now = clock.instant();
        String caseId = "case-" + payload.applicationId();
        MockEvaluationResult result = evaluator.evaluate(payload.applicationId(), caseId, payload.inputSnapshotVersion());

        DecisionCaseEntity decisionCase = decisionCases.saveAndFlush(new DecisionCaseEntity(
                caseId,
                payload.applicationId(),
                payload.applicantId(),
                payload.inputSnapshotVersion(),
                now
        ));
        outbox.save(reviewStartedEvent(decisionCase, event, now));

        decisionCase.transitionTo(DecisionCaseStatus.EVALUATION_REQUESTED, now);
        EvaluationRunEntity run = evaluationRuns.save(new EvaluationRunEntity(
                result.evaluationRunId(),
                decisionCase,
                MockDecisionEvaluator.RULE_VERSION,
                payload.inputSnapshotVersion(),
                result.decisionId(),
                now
        ));
        OutboxEventEntity requested = evaluationRequestedEvent(decisionCase, run, event.eventId(), now);
        outbox.save(requested);

        run.complete(result.proposal(), result.confidence(), json.canonicalJson(result.reasonCodes()), now);
        decisionCase.completeEvaluation(now);
        OutboxEventEntity completed = evaluationCompletedEvent(decisionCase, run, requested.getEventId(), now);
        outbox.save(completed);

        decisionCase.commandDecision(result.proposal(), result.assuranceResult(), now);
        outbox.save(decisionCommandedEvent(decisionCase, run, result, completed.getEventId(), now));
        recordInbox(event);
        log.info("Decision case created applicationId={} caseId={} evaluationRunId={} finalDecision={}",
                payload.applicationId(), caseId, run.getEvaluationRunId(), result.proposal());
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

    private void validateSubmitted(EventEnvelope event, LoanApplicationSubmittedPayload payload) {
        if (!"loan-service".equals(event.producer())) {
            throw new IllegalArgumentException("Submitted event producer must be loan-service");
        }
        if (!event.applicationId().equals(payload.applicationId())) {
            throw new IllegalArgumentException("Envelope and payload applicationId differ");
        }
        if (!event.applicationId().toString().equals(event.correlationId())) {
            throw new IllegalArgumentException("Phase 1 correlationId must equal applicationId");
        }
        if (payload.inputSnapshotVersion() == null || payload.inputSnapshotVersion().isBlank()) {
            throw new IllegalArgumentException("Submitted event requires inputSnapshotVersion");
        }
        if (payload.applicantId() == null || payload.applicantId().isBlank()) {
            throw new IllegalArgumentException("Submitted event requires applicantId");
        }
    }

    private void requireEvent(EventEnvelope event, String eventType, String schemaVersion) {
        if (!eventType.equals(event.eventType()) || !schemaVersion.equals(event.schemaVersion())) {
            throw new IllegalArgumentException("Unsupported event contract: " + event.eventType() + " " + event.schemaVersion());
        }
    }

    private void recordInbox(EventEnvelope event) {
        try {
            inbox.save(new InboxEventEntity(
                    event.eventId(),
                    event.eventType(),
                    event.applicationId(),
                    json.sha256(event.payload().toString()),
                    clock.instant()
            ));
        } catch (DataIntegrityViolationException ignoredDuplicate) {
            log.info("Duplicate inbox event ignored eventId={}", event.eventId());
        }
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
        payload.put("executionPlanVersion", run.getRuleVersion());
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
                                                     MockEvaluationResult result, UUID causationId, Instant now) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("commandId", commandId(decisionCase.getCaseId(), run.getEvaluationRunId()).toString());
        payload.put("decisionCaseId", decisionCase.getCaseId());
        payload.put("applicationId", decisionCase.getApplicationId().toString());
        payload.put("decisionId", run.getDecisionId().toString());
        payload.put("evaluationRunId", run.getEvaluationRunId().toString());
        payload.put("evaluationRunStatus", "COMPLETED");
        payload.put("finalDecision", result.proposal().name());
        payload.put("assuranceResult", result.assuranceResult());
        payload.put("reasonCodes", List.of("GOVERNANCE_VERIFIED_PROPOSAL"));
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
        EvaluationRunEntity run = evaluationRuns.findFirstByDecisionCaseCaseIdOrderByRequestedAtDesc(decisionCase.getCaseId())
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
}
