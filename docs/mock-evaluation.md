# Mock Evaluation

Phase 1 uses deterministic mock evaluation only.

The rule seed is:

```text
applicationId:caseId:inputSnapshotVersion:phase1-mock-v1
```

The seed deterministically derives:

- evaluation run id
- decision id
- proposal
- confidence
- reason codes

This is not a financial credit policy. It does not call an LLM, ML model, OPA, or Agent Runtime. It is a stable Phase 1 boundary and is expected to be replaced in Phase 2.
