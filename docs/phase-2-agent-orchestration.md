# Phase 2 Agent Orchestration

Governance owns the execution plan, `evaluationRunId`, `agentRunId`, request idempotency key, timeout and retry orchestration, Agent Result validation, state transition, and transactional outbox audit event.

The normal path is:

1. Create a Decision Case and write `governance.review.started.v1`.
2. Create an Evaluation Run with immutable snapshot/model identity.
3. Validate the Loan Decision Agent Request with Contracts.
4. Record the attempt, next retry time, and execution lease in the database.
5. Call `POST /internal/v1/loan-decision-agent/runs`.
6. Validate the Agent Result with Contracts.
7. Compare immutable request/result fields.
8. Store the accepted proposal snapshot only after validation.
9. Emit `governance.agent-result.validated.v1` as `VALIDATED` or `REJECTED`.

Governance does not compute model scores, SHAP values, final Loan status, or fallback proposals. `loan.decision.commanded.v1` is not emitted from the Phase 2 proposal-only path.

If Governance commits the inbox row and crashes before or during the external Agent Runtime call, duplicate delivery of the same Loan submitted event resumes the existing `RUNNING` Evaluation Run instead of skipping it. A scheduled recovery loop also scans `RUNNING` Evaluation Runs whose lease expired and whose `nextAttemptAt` has arrived.

Transport timeout or connection failure is recorded as Governance orchestration failure state and retry metadata on `evaluation_run`. It is not treated as an Agent Runtime `FAILED` result and does not emit `governance.agent-result.validated.v1` unless a real Agent result payload is available or the failure is a Governance validation rejection.

Known production blocker: the current Phase 1 loan submitted event does not yet provide a full versioned financial snapshot contract or materialized Phase 2 feature payload. Governance currently sends a contract-valid `IMMUTABLE_REFERENCE` request and does not synthesize Loan features from the submitted event. The current Agent Runtime requires feature payload materialization, so production Phase 2 E2E requires either:

- Loan Service snapshot/feature provider integration before Governance emits `MATERIALIZED_FEATURES` requests.
- Agent Runtime snapshot repository support for `IMMUTABLE_REFERENCE` requests.

Until one of those boundaries is implemented, Governance-Agent Runtime E2E remains blocked even though Governance request/result validation and orchestration state handling are covered locally.
