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

`evaluation_run` stores the deterministic mock run in the Contracts v2.0.0 shape: execution plan version, component versions, policy input/bundle versions, supersedes run id, status, creation/completion times, proposal, confidence, and generated decision id.

## Inbox and Outbox

`inbox_event` records consumed event ids after successful processing. `outbox_event` stores events atomically with domain changes and is published asynchronously.

`governance_event_quarantine` records malformed or unsupported-version events that must not be retried blindly.
