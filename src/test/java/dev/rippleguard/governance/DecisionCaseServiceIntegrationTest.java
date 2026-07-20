package dev.rippleguard.governance;

import static org.assertj.core.api.Assertions.assertThat;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rippleguard.governance.application.DecisionCaseService;
import dev.rippleguard.governance.application.EventEnvelope;
import dev.rippleguard.governance.application.MockDecisionEvaluator;
import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.domain.EvaluationRunStatus;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseRepository;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunRepository;
import dev.rippleguard.governance.infrastructure.persistence.GovernanceEventQuarantineRepository;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(properties = "debug=false")
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
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from evaluation_run");
        jdbc.update("delete from outbox_event");
        jdbc.update("delete from inbox_event");
        jdbc.update("delete from governance_event_quarantine");
        jdbc.update("delete from decision_case");
    }

    @Test
    void submittedEventCreatesCaseEvaluationAndDecisionCommandOutbox() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000001");
        EventEnvelope submitted = submitted(applicationId);
        service.handleLoanApplicationSubmitted(submitted);

        var response = service.getByApplication(applicationId);

        assertThat(response.status()).isEqualTo(DecisionCaseStatus.RESOLVED);
        assertThat(response.evaluationRunId()).isNotNull();
        assertThat(response.evaluationRunStatus()).isEqualTo(EvaluationRunStatus.COMPLETED);
        assertThat(response.finalDecision()).isNotNull();
        assertThat(decisionCases.findByApplicationId(applicationId)).isPresent();
        assertThat(evaluationRuns.findFirstByDecisionCaseCaseIdOrderByCreatedAtDesc(response.caseId())).isPresent();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsExactlyInAnyOrder(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "agent.evaluation.completed.v1",
                        "loan.decision.commanded.v1"
                );

        List<OutboxRow> rows = outboxRowsByCreatedAt();
        assertThat(rows).extracting(OutboxRow::eventType)
                .containsExactly(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "agent.evaluation.completed.v1",
                        "loan.decision.commanded.v1"
                );
        assertThat(rows.get(0).createdAt()).isBefore(rows.get(1).createdAt());
        assertThat(rows.get(1).createdAt()).isBefore(rows.get(2).createdAt());
        assertThat(rows.get(2).createdAt()).isBefore(rows.get(3).createdAt());

        List<Instant> occurredAt = rows.stream()
                .map(row -> occurredAt(row.payload()))
                .toList();
        assertThat(occurredAt.get(0)).isBefore(occurredAt.get(1));
        assertThat(occurredAt.get(1)).isBefore(occurredAt.get(2));
        assertThat(occurredAt.get(2)).isBefore(occurredAt.get(3));

        Map<String, UUID> eventIdsByType = rows.stream()
                .collect(java.util.stream.Collectors.toMap(
                        OutboxRow::eventType,
                        row -> eventId(row.payload())
                ));
        assertThat(causationId(payloadFor(rows, "governance.review.started.v1")))
                .isEqualTo(submitted.eventId());
        assertThat(causationId(payloadFor(rows, "agent.evaluation.requested.v1")))
                .isEqualTo(submitted.eventId());
        assertThat(causationId(payloadFor(rows, "agent.evaluation.completed.v1")))
                .isEqualTo(eventIdsByType.get("agent.evaluation.requested.v1"));
        assertThat(causationId(payloadFor(rows, "loan.decision.commanded.v1")))
                .isEqualTo(eventIdsByType.get("agent.evaluation.completed.v1"));
    }

    @Test
    void duplicateSubmittedEventIsIdempotent() {
        EventEnvelope event = submitted(UUID.fromString("10000000-0000-4000-8000-000000000002"));

        service.handleLoanApplicationSubmitted(event);
        service.handleLoanApplicationSubmitted(event);

        assertThat(decisionCases.count()).isEqualTo(1);
        assertThat(outbox.count()).isEqualTo(4);
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
    void blockedSnapshotCreatesBlockedCaseAndNoDecisionCommand() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000006");

        service.handleLoanApplicationSubmitted(submitted(applicationId, "snapshot-v-blocked-001"));

        var response = service.getByApplication(applicationId);
        assertThat(response.status()).isEqualTo(DecisionCaseStatus.BLOCKED);
        assertThat(outbox.findAll()).extracting("eventType")
                .doesNotContain("loan.decision.commanded.v1");
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
        return event(applicationId, Map.of(
                "applicationId", applicationId.toString(),
                "applicantId", "customer-42",
                "inputSnapshotVersion", snapshotVersion,
                "submittedAt", Instant.now().toString(),
                "submissionChannel", "WEB"
        ));
    }

    private EventEnvelope event(UUID applicationId, Map<String, Object> payload) {
        return new EventEnvelope(
                UUID.randomUUID(),
                "loan.application.submitted.v1",
                "1.1.0",
                Instant.now(),
                "loan-service",
                applicationId,
                applicationId.toString(),
                null,
                applicationId.toString(),
                null,
                objectMapper.valueToTree(payload)
        );
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
