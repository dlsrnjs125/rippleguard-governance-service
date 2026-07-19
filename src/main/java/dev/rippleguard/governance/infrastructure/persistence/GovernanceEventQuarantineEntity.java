package dev.rippleguard.governance.infrastructure.persistence;

import dev.rippleguard.governance.domain.QuarantineFailureCode;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "governance_event_quarantine")
public class GovernanceEventQuarantineEntity {
    @Id
    @Column(name = "quarantine_id")
    private UUID quarantineId;

    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "event_type", length = 128)
    private String eventType;

    @Column(name = "schema_version", length = 32)
    private String schemaVersion;

    @Column(name = "payload_hash", nullable = false, length = 64)
    private String payloadHash;

    @Enumerated(EnumType.STRING)
    @Column(name = "failure_code", nullable = false, length = 64)
    private QuarantineFailureCode failureCode;

    @Column(name = "failure_message", nullable = false, columnDefinition = "text")
    private String failureMessage;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(nullable = false)
    private boolean retryable;

    protected GovernanceEventQuarantineEntity() {
    }

    public GovernanceEventQuarantineEntity(UUID quarantineId, UUID eventId, String eventType, String schemaVersion,
                                           String payloadHash, QuarantineFailureCode failureCode,
                                           String failureMessage, Instant receivedAt, boolean retryable) {
        this.quarantineId = quarantineId;
        this.eventId = eventId;
        this.eventType = eventType;
        this.schemaVersion = schemaVersion;
        this.payloadHash = payloadHash;
        this.failureCode = failureCode;
        this.failureMessage = failureMessage;
        this.receivedAt = receivedAt;
        this.retryable = retryable;
    }
}
