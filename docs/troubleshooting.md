# Troubleshooting

## Event Is Not Processed

Check schema version, producer, application id, and correlation id. Unknown contract versions are rejected and should not be treated as completed inbox processing.

## Snapshot Reference Missing

`inputSnapshotVersion` is required for evaluation. If the envelope and core business identifiers are valid but the snapshot reference is missing, Governance creates a case in `VERIFICATION_REQUIRED`, records reason `SNAPSHOT_REFERENCE_MISSING`, and records the inbox event to stop endless Kafka retry.

## Outbox Event Remains Pending

Kafka publish failure does not roll back the domain transaction. The event remains retryable in `outbox_event`.

## Duplicate Event

Duplicate event ids are ignored after the first successful inbox record. A different event id with the same application and different payload marks the case `RECALCULATION_REQUIRED`.

## Audit Timeline Is Partial

Infra `make phase1-duplicate-check` can fail if the Audit timeline reports `EVENT_GAP_DETECTED` and `INVALID_REFERENCE` even though all six Phase 1 events were persisted. One observed cause was Governance emitting multiple causally related events with the same `occurredAt` and outbox `created_at` values. PostgreSQL outbox claiming and Audit timeline sorting could then place `loan.decision.commanded.v1` before its causation event `agent.evaluation.completed.v1`.

Governance fixes this by assigning transaction-local timestamps that strictly increase in logical order:

```text
governance.review.started.v1
agent.evaluation.requested.v1
agent.evaluation.completed.v1
loan.decision.commanded.v1
```

This is a local ordering contract for one Loan Application transaction, not a system-wide clock precision guarantee. Do not work around this by weakening Infra checks, rewriting Kafka events, sorting by event type, or ignoring Audit `INVALID_REFERENCE` warnings.

Registry digest pinning, SBOM, and SLSA provenance remain deferred.

## Unknown Version

Unknown schema versions are stored in `governance_event_quarantine` as non-retryable records instead of being acknowledged in the inbox.
