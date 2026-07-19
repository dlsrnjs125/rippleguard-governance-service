package dev.rippleguard.governance.application;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record EventEnvelope(
        UUID eventId,
        String eventType,
        String schemaVersion,
        Instant occurredAt,
        String producer,
        UUID applicationId,
        String caseId,
        UUID evaluationRunId,
        String correlationId,
        UUID causationId,
        JsonNode payload
) {
}
