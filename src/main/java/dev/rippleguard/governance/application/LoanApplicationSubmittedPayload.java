package dev.rippleguard.governance.application;

import java.time.Instant;
import java.util.UUID;

public record LoanApplicationSubmittedPayload(
        UUID applicationId,
        String applicantId,
        String inputSnapshotVersion,
        Instant submittedAt,
        String submissionChannel
) {
}
