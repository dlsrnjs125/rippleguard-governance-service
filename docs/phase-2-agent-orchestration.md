# Phase 2 Agent Orchestration

Governance owns the execution plan, `evaluationRunId`, `agentRunId`, request idempotency key, timeout and retry orchestration, Agent Result validation, state transition, and transactional outbox audit event.

The normal path is:

1. Create a Decision Case and write `governance.review.started.v1`.
2. Create an Evaluation Run with immutable snapshot/model identity.
3. Validate the Loan Decision Agent Request with Contracts.
4. Call `POST /internal/v1/loan-decision-agent/runs`.
5. Validate the Agent Result with Contracts.
6. Compare immutable request/result fields.
7. Store the accepted proposal snapshot only after validation.
8. Emit `governance.agent-result.validated.v1` as `VALIDATED` or `REJECTED`.

Governance does not compute model scores, SHAP values, final Loan status, or fallback proposals. `loan.decision.commanded.v1` is not emitted from the Phase 2 proposal-only path.

Known limitation: the current Phase 1 loan submitted event does not yet provide a full versioned financial snapshot contract. Governance therefore records a deterministic snapshot reference from the submitted event boundary. A Loan Service snapshot provider is a follow-up before production Phase 2 completion.
