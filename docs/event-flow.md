# Event Flow

1. Consume `loan.application.submitted.v1`.
2. Validate schema version, producer, application id, correlation id, and snapshot reference.
3. Create `DecisionCase`.
4. Write `governance.review.started.v1` to outbox.
5. Create `EvaluationRun` with fixed `agentRunId`, request idempotency key, snapshot identity, model version, artifact digest, preprocessing version, threshold version, attempts, and deadline.
6. Validate the Agent Request against Contracts.
7. Call Loan Decision Agent Runtime outside the DB transaction.
8. Validate the Agent Result against Contracts and immutable request identity.
9. If validation passes for a completed result, store the accepted proposal snapshot, move the case to `PROPOSAL_READY`, and write `governance.agent-result.validated.v1` with `VALIDATED`.
10. If validation fails or the Agent returns a failed result, move the case to `VERIFICATION_REQUIRED` or `BLOCKED` and write `governance.agent-result.validated.v1` with `REJECTED`.

Kafka publish failure leaves rows in the outbox for retry. DB failure prevents inbox completion. Unknown schema versions are quarantined and are not treated as completed inbox records.

Governance does not publish `agent.evaluation.completed.v1` or `loan.decision.commanded.v1` in the Phase 2 Agent orchestration path.
