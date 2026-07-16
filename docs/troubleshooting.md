# Troubleshooting

## Event Is Not Processed

Check schema version, producer, application id, and correlation id. Unknown contract versions are rejected and should not be treated as completed inbox processing.

## Snapshot Reference Missing

`inputSnapshotVersion` is required for evaluation. If the envelope and core business identifiers are valid but the snapshot reference is missing, Governance creates a case in `VERIFICATION_REQUIRED`, records reason `SNAPSHOT_REFERENCE_MISSING`, and records the inbox event to stop endless Kafka retry.

## Outbox Event Remains Pending

Kafka publish failure does not roll back the domain transaction. The event remains retryable in `outbox_event`.

## Duplicate Event

Duplicate event ids are ignored after the first successful inbox record. A different event id with the same application and different payload marks the case `RECALCULATION_REQUIRED`.

## Unknown Version

Unknown schema versions are stored in `governance_event_quarantine` as non-retryable records instead of being acknowledged in the inbox.
