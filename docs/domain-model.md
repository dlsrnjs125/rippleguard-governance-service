# Domain Model

## Decision Case

`decision_case` is keyed by `case_id` and has a unique `application_id`.

States:

- `REVIEW_STARTED`
- `EVALUATION_REQUESTED`
- `EVALUATION_COMPLETED`
- `DECISION_COMMANDED`
- `FAILED`

## Evaluation Run

`evaluation_run` stores the deterministic mock run, rule version, input snapshot version, proposal, confidence, and generated decision id.

## Inbox and Outbox

`inbox_event` records consumed event ids after successful processing. `outbox_event` stores events atomically with domain changes and is published asynchronously.
