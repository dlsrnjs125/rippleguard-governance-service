# Domain Model

## Decision Case

`decision_case` is keyed by `case_id` and has a unique `application_id`.

States:

- `CREATED`
- `PREFLIGHT_COMPLETED`
- `EVALUATION_REQUESTED`
- `PROPOSAL_READY`
- `ASSURANCE_EVALUATED`
- `VERIFICATION_REQUIRED`
- `BLOCKED`
- `RECALCULATION_REQUIRED`
- `RESOLVED`

## Evaluation Run

`evaluation_run` stores the Phase 2 Agent orchestration record: execution plan version, component versions, fixed `agentRunId`, request idempotency key, snapshot identity, model and preprocessing versions, attempt count, deadline, accepted result digest, accepted proposal snapshot, validation outcome, and failure classification/reason when rejected.

## Inbox and Outbox

`inbox_event` records consumed event ids after successful processing. `outbox_event` stores events atomically with domain changes and is published asynchronously.

`governance_event_quarantine` records malformed or unsupported-version events that must not be retried blindly.
