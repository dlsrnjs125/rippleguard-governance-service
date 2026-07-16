package dev.rippleguard.governance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rippleguard.governance.application.DecisionCaseService;
import dev.rippleguard.governance.application.EventEnvelope;
import dev.rippleguard.governance.application.MockDecisionEvaluator;
import dev.rippleguard.governance.domain.DecisionCaseStatus;
import dev.rippleguard.governance.infrastructure.persistence.DecisionCaseRepository;
import dev.rippleguard.governance.infrastructure.persistence.EvaluationRunRepository;
import dev.rippleguard.governance.infrastructure.persistence.OutboxEventRepository;
import java.time.Instant;
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
    ObjectMapper objectMapper;

    @Autowired
    JdbcTemplate jdbc;

    @BeforeEach
    void cleanDatabase() {
        jdbc.update("delete from evaluation_run");
        jdbc.update("delete from outbox_event");
        jdbc.update("delete from inbox_event");
        jdbc.update("delete from decision_case");
    }

    @Test
    void submittedEventCreatesCaseEvaluationAndDecisionCommandOutbox() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000001");
        service.handleLoanApplicationSubmitted(submitted(applicationId));

        var response = service.getByApplication(applicationId);

        assertThat(response.status()).isEqualTo(DecisionCaseStatus.DECISION_COMMANDED);
        assertThat(response.evaluationRunId()).isNotNull();
        assertThat(response.finalDecision()).isNotNull();
        assertThat(decisionCases.findByApplicationId(applicationId)).isPresent();
        assertThat(evaluationRuns.findFirstByDecisionCaseCaseIdOrderByRequestedAtDesc(response.caseId())).isPresent();
        assertThat(outbox.findAll()).extracting("eventType")
                .containsExactlyInAnyOrder(
                        "governance.review.started.v1",
                        "agent.evaluation.requested.v1",
                        "agent.evaluation.completed.v1",
                        "loan.decision.commanded.v1"
                );
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
    void rejectsUnsupportedSchemaVersion() {
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

        assertThatThrownBy(() -> service.handleLoanApplicationSubmitted(badVersion))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unsupported event contract");
        assertThat(decisionCases.count()).isZero();
    }

    @Test
    void submittedEventRequiresSnapshotReference() {
        UUID applicationId = UUID.fromString("10000000-0000-4000-8000-000000000005");
        EventEnvelope event = event(applicationId, Map.of(
                "applicationId", applicationId.toString(),
                "applicantId", "customer-42",
                "submittedAt", Instant.now().toString(),
                "submissionChannel", "WEB"
        ));

        assertThatThrownBy(() -> service.handleLoanApplicationSubmitted(event))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("inputSnapshotVersion");
    }

    private EventEnvelope submitted(UUID applicationId) {
        return event(applicationId, Map.of(
                "applicationId", applicationId.toString(),
                "applicantId", "customer-42",
                "inputSnapshotVersion", "snapshot-v1",
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
}
