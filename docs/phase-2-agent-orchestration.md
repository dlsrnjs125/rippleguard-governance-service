# Phase 2 Agent Orchestration

Governance owns the execution plan, `evaluationRunId`, `agentRunId`, request idempotency key, timeout and retry orchestration, Agent Result validation, state transition, and transactional outbox audit event.

The normal path is:

1. Create a Decision Case and write `governance.review.started.v1`.
2. Resolve and verify the immutable Phase 2 Feature Snapshot from Loan Service.
3. Create an Evaluation Run with immutable snapshot, feature payload, and model identity.
4. Validate the materialized Loan Decision Agent Request with Contracts.
5. Persist `agent.evaluation.requested.v1` to the transactional outbox and store its `eventId`.
6. Record the attempt, next retry time, and execution lease in the database.
7. Call `POST /internal/v1/loan-decision-agent/runs`.
8. Validate the Agent Result with Contracts.
9. Compare immutable request/result fields.
10. Store the accepted proposal snapshot only after validation.
11. Emit `governance.agent-result.validated.v1` as `VALIDATED` or `REJECTED` with `causationId` equal to the persisted request event id.

Governance does not compute model scores, SHAP values, final Loan status, or fallback proposals. `loan.decision.commanded.v1` is not emitted from the Phase 2 proposal-only path.

If Governance commits the inbox row and crashes before or during the external Agent Runtime call, duplicate delivery of the same Loan submitted event resumes the existing `RUNNING` Evaluation Run instead of skipping it. A scheduled recovery loop also scans `RUNNING` Evaluation Runs whose lease expired and whose `nextAttemptAt` has arrived.

Transport timeout or connection failure is recorded as Governance orchestration failure state and retry metadata on `evaluation_run`. It is not treated as an Agent Runtime `FAILED` result and does not emit `governance.agent-result.validated.v1` unless a real Agent result payload is available or the failure is a Governance validation rejection.

Loan Feature Snapshot timeout is retryable before Governance persists a new Evaluation Run. Snapshot 404 is mapped to verification required. Snapshot or Feature Payload digest mismatch is blocked. Feature Payload contract invalidity is mapped to validation required.

The Governance outbox enforces publication ordering only for Governance-owned predecessor events stored in the same table. External Loan events are ordered by Kafka key and reconciled by Audit timeline policies.
