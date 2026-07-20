# Event Flow

1. Consume `loan.application.submitted.v1`.
2. Validate schema version, producer, application id, correlation id, and snapshot reference.
3. Create `DecisionCase`.
4. Write `governance.review.started.v1` to outbox.
5. Create `EvaluationRun`.
6. Write `agent.evaluation.requested.v1` to outbox.
7. Complete deterministic mock evaluation.
8. Write `agent.evaluation.completed.v1` to outbox.
9. Complete mock assurance.
10. Evaluate mock assurance.
11. If assurance is complete, write `loan.decision.commanded.v1` to outbox and resolve the case.
12. If assurance is incomplete or violated, move to `VERIFICATION_REQUIRED` or `BLOCKED` and do not emit a decision command.

Kafka publish failure leaves rows in the outbox for retry. DB failure prevents inbox completion. Unknown schema versions are quarantined and are not treated as completed inbox records.

Within one submitted-event transaction, Governance assigns strictly increasing occurrence and outbox timestamps to `governance.review.started.v1`, `agent.evaluation.requested.v1`, `agent.evaluation.completed.v1`, and `loan.decision.commanded.v1`. This timestamp sequence expresses logical causation order inside the transaction; it is not intended to be a system-wide clock accuracy guarantee.
