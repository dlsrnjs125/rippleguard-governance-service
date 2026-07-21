# Event Flow

1. Consume `loan.application.submitted.v1`.
2. Validate schema version, producer, application id, correlation id, and snapshot reference.
3. Create `DecisionCase`.
4. Write `governance.review.started.v1` to outbox.
5. Create `EvaluationRun` with fixed `agentRunId`, request idempotency key, snapshot identity, model version, artifact digest, preprocessing version, threshold version, attempts, retry metadata, lease metadata, and deadline.
6. Validate the Agent Request against Contracts.
7. Record the attempt and execution lease in the DB, then call Loan Decision Agent Runtime outside the DB transaction.
8. Validate the Agent Result against Contracts and immutable request identity.
9. If validation passes for a completed result, store the accepted proposal snapshot, move the case to `PROPOSAL_READY`, and write `governance.agent-result.validated.v1` with `VALIDATED`.
10. If validation fails or the Agent returns a failed result, move the case to `VERIFICATION_REQUIRED` or `BLOCKED` and write `governance.agent-result.validated.v1` with `REJECTED`.

Kafka publish failure leaves rows in the outbox for retry. Unknown schema versions are quarantined and are not treated as completed inbox records. If the inbox row is committed but the external Agent Runtime call does not complete, duplicate event delivery and the scheduled recovery loop resume the existing `RUNNING` Evaluation Run instead of treating the inbox row as complete business processing.

Governance does not publish `agent.evaluation.completed.v1` or `loan.decision.commanded.v1` in the Phase 2 Agent orchestration path.

Governance currently emits contract-valid `IMMUTABLE_REFERENCE` Agent Requests. Actual Phase 2 E2E remains blocked until Loan materialized feature payloads are available to Governance or Agent Runtime can resolve immutable snapshot references itself.
