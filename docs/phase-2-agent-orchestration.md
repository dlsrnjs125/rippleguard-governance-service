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
11. Emit `governance.agent-result.validated.v2` as `VALIDATED` or `REJECTED` with `causationId` equal to the persisted request event id.

Governance does not compute model scores, SHAP values, final Loan status, or fallback proposals. `loan.decision.commanded.v1` is not emitted from the Phase 2 proposal-only path.

If Governance commits the inbox row and crashes before or during the external Agent Runtime call, duplicate delivery of the same Loan submitted event resumes the existing `RUNNING` Evaluation Run instead of skipping it. A scheduled recovery loop also scans `RUNNING` Evaluation Runs whose lease expired and whose `nextAttemptAt` has arrived.

Transport timeout or connection failure is recorded as Governance orchestration failure state and retry metadata on `evaluation_run`. It is not treated as an Agent Runtime `FAILED` result and does not emit `governance.agent-result.validated.v2` unless a real Agent result payload is available or the failure is a Governance validation rejection.

Loan Feature Snapshot timeout is retryable before Governance persists a new Evaluation Run. Snapshot 404 is mapped to verification required. Snapshot or Feature Payload digest mismatch is blocked. Feature Payload contract invalidity is mapped to validation required.

## Validation Event Versioning

Phase 2 Agent orchestration publishes `governance.agent-result.validated.v2` only. Governance does not dual-publish the previous V1 validation event because two validation events for the same `agentRunId` can create duplicate Audit timeline projections. Migration is handled by routing the Phase 2 validation topic/consumer to V2 while older V1 rows remain historical outbox data.

The V2 payload is provenance-complete and is built only from the persisted `EvaluationRun`, the validated Agent Result, and the persisted `agent.evaluation.requested.v1` `eventId`. If persisted snapshot, model, preprocessing, threshold, or request-event provenance is missing, Governance blocks the Evaluation Run and does not emit a validation event.

The Governance outbox enforces publication ordering only for Governance-owned predecessor events stored in the same table. External Loan events are ordered by Kafka key and reconciled by Audit timeline policies.

## Deployment Order

V2-only production must be introduced in consumer-first order:

1. Deploy Audit Replay with V1 and V2 validation-event consumer support.
2. Verify V2 contract validation and Agent Run projection handling in Audit.
3. Deploy Governance with the V2-only producer.
4. Verify Governance creates no new `governance.agent-result.validated.v1` outbox rows.
5. Allow historical unpublished V1 outbox rows to drain as V1 historical data. Do not convert them to V2.

Rollback is allowed only while Audit continues to accept V1 validation events. Existing V2 outbox rows must not be rewritten as V1. If historical V1 rows and newer V2 rows exist for the same `agentRunId`, Audit must route by event type and apply its timeline de-duplication policy instead of treating V1/V2 dual rows as a valid new producer mode.

The cross-repository deployment contract is tracked in `rippleguard-docs/phases/phase-02-loan-decision/v2-validation-event-migration.md`.
