# Event Flow

1. Consume `loan.application.submitted.v1`.
2. Validate schema version, producer, application id, correlation id, and snapshot reference.
3. Resolve the immutable Phase 2 Feature Snapshot from Loan Service by the submitted snapshot reference.
4. Verify Snapshot identity, Feature Payload contract, `snapshotDigest`, and `featurePayloadDigest`.
5. Create `DecisionCase`.
6. Write `governance.review.started.v1` to outbox.
7. Create `EvaluationRun` with fixed `agentRunId`, request idempotency key, snapshot identity, feature payload identity, model version, artifact digest, preprocessing version, threshold version, attempts, retry metadata, lease metadata, and deadline.
8. Validate the materialized Agent Request against Contracts.
9. Write the durable `agent.evaluation.requested.v1` request event to outbox and store its `eventId` on the Evaluation Run.
10. Record the attempt and execution lease in the DB, then call Loan Decision Agent Runtime outside the DB transaction.
11. Validate the Agent Result against Contracts and immutable request identity.
12. If validation passes for a completed result, store the accepted proposal snapshot, move the case to `PROPOSAL_READY`, and write `governance.agent-result.validated.v2` with `VALIDATED`.
13. If validation fails or the Agent returns a failed result, move the case to `VERIFICATION_REQUIRED` or `BLOCKED` and write `governance.agent-result.validated.v2` with `REJECTED`.

Kafka publish failure leaves rows in the outbox for retry. Unknown schema versions are quarantined and are not treated as completed inbox records. If the inbox row is committed but the external Agent Runtime call does not complete, duplicate event delivery and the scheduled recovery loop resume the existing `RUNNING` Evaluation Run instead of treating the inbox row as complete business processing.

Governance does not publish `agent.evaluation.completed.v1` or `loan.decision.commanded.v1` in the Phase 2 Agent orchestration path.

Governance emits `MATERIALIZED_FEATURES` Agent Requests. It does not fall back to current Loan Application state, cached prior payloads, latest snapshots, mock proposals, or default proposals when Feature Snapshot acquisition fails.

Outbox publication ordering is guaranteed for Governance-owned predecessors stored in the same outbox table. For example, `governance.agent-result.validated.v2` is not claimable until its causative `agent.evaluation.requested.v1` outbox row is published. External predecessors such as Loan Service submitted events are not stored in the Governance outbox, so their ordering depends on Kafka key ordering and downstream Audit out-of-order handling.
