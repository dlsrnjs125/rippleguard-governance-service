# Mock Evaluation

Phase 1 used deterministic mock evaluation only.

The rule is fixture-driven by `inputSnapshotVersion`:

```text
snapshot-v-reject-*  -> REJECT
snapshot-v-blocked-* -> ASSURANCE_VIOLATED and BLOCKED
snapshot-v-verify-*  -> ASSURANCE_INCOMPLETE and VERIFICATION_REQUIRED
all other snapshots  -> APPROVE
```

The case id, snapshot version, and fixed rule version deterministically derive:

- evaluation run id
- decision id
- proposal
- confidence
- reason codes

This is not a financial credit policy. It does not call an LLM, ML model, OPA, or Agent Runtime. It is historical Phase 1 context and is not on the production Phase 2 Agent orchestration path.

Phase 1 executes the mock request, mock completion, mock assurance, and emitted trace events in one database transaction. The requested/completed events are trace-contract artifacts, not evidence that an external Agent Runtime evaluated the case. If the transaction rolls back, all mock trace outbox rows roll back together.

Phase 2 must not fall back to this mock path when Agent Runtime is unavailable, returns a failed result, times out, or violates Contracts.
