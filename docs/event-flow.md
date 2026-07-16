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
10. Write `loan.decision.commanded.v1` to outbox.

Kafka publish failure leaves rows in the outbox for retry. DB failure prevents inbox completion.
