# Troubleshooting

## Event Is Not Processed

Check schema version, producer, application id, and correlation id. Unknown contract versions are rejected and should not be treated as completed inbox processing.

## Snapshot Reference Missing

`inputSnapshotVersion` is required. Missing snapshot references keep the event out of the inbox so it can be corrected and replayed.

## Outbox Event Remains Pending

Kafka publish failure does not roll back the domain transaction. The event remains retryable in `outbox_event`.

## Duplicate Event

Duplicate event ids are ignored after the first successful inbox record.
