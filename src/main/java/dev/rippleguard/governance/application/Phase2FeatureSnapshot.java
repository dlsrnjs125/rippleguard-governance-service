package dev.rippleguard.governance.application;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record Phase2FeatureSnapshot(
        String schemaVersion,
        UUID snapshotId,
        UUID applicationId,
        String snapshotVersion,
        String snapshotSchemaVersion,
        String featureSchemaVersion,
        JsonNode snapshotReference,
        JsonNode featurePayload,
        String featurePayloadDigest,
        int sourceLoanApplicationVersion,
        Instant createdAt
) {
}
