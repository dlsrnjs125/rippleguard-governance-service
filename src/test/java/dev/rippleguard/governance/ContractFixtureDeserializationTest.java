package dev.rippleguard.governance;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import dev.rippleguard.governance.application.EventEnvelope;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ContractFixtureDeserializationTest {
    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void deserializesPhaseOneEventExamples() throws Exception {
        EventEnvelope submitted = event("""
                {
                  "eventId": "10000000-0000-4000-8000-000000000001",
                  "eventType": "loan.application.submitted.v1",
                  "schemaVersion": "1.1.0",
                  "occurredAt": "2026-01-15T09:00:00+09:00",
                  "producer": "loan-service",
                  "applicationId": "10000000-0000-4000-8000-000000000001",
                  "caseId": "10000000-0000-4000-8000-000000000001",
                  "evaluationRunId": null,
                  "correlationId": "10000000-0000-4000-8000-000000000001",
                  "causationId": null,
                  "payload": { "applicationId": "10000000-0000-4000-8000-000000000001", "applicantId": "customer-42", "inputSnapshotVersion": "snapshot-v1", "submittedAt": "2026-01-15T09:00:00+09:00", "submissionChannel": "WEB" }
                }
                """);
        EventEnvelope commanded = event("""
                {
                  "eventId": "10000000-0000-4000-8000-000000000007",
                  "eventType": "loan.decision.commanded.v1",
                  "schemaVersion": "1.1.0",
                  "occurredAt": "2026-01-15T09:07:00+09:00",
                  "producer": "governance-service",
                  "applicationId": "10000000-0000-4000-8000-000000000001",
                  "caseId": "case-1001",
                  "evaluationRunId": "30000000-0000-4000-8000-000000000001",
                  "correlationId": "10000000-0000-4000-8000-000000000001",
                  "causationId": "10000000-0000-4000-8000-000000000006",
                  "payload": { "commandId": "50000000-0000-4000-8000-000000000001", "decisionCaseId": "case-1001", "applicationId": "10000000-0000-4000-8000-000000000001", "decisionId": "40000000-0000-4000-8000-000000000001", "evaluationRunId": "30000000-0000-4000-8000-000000000001", "evaluationRunStatus": "COMPLETED", "finalDecision": "APPROVE", "assuranceResult": "ASSURANCE_COMPLETE", "reasonCodes": ["GOVERNANCE_VERIFIED_PROPOSAL"], "issuedAt": "2026-01-15T09:07:00+09:00", "idempotencyKey": "decision-command-case-1001" }
                }
                """);

        assertThat(submitted.eventType()).isEqualTo("loan.application.submitted.v1");
        assertThat(submitted.applicationId()).isEqualTo(UUID.fromString("10000000-0000-4000-8000-000000000001"));
        assertThat(commanded.payload().get("idempotencyKey").asText()).isEqualTo("decision-command-case-1001");
    }

    private EventEnvelope event(String json) throws Exception {
        return objectMapper.readValue(json, EventEnvelope.class);
    }
}
