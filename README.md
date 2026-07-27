# RippleGuard Governance Service

Governance Core service. It owns Decision Case and Evaluation Run state, consumes Loan application submissions, orchestrates the Phase 2 Loan Decision Agent Runtime, validates Agent results against Contracts, and emits Governance validation audit events through a transactional outbox.

## Baselines

- Contracts: `dlsrnjs125/rippleguard-contracts@5781bd30f688c25ae0d531049d6d7fb39ec3e9b1`
- Loan Service: `dlsrnjs125/rippleguard-loan-service@1f78f8c3358fc0437b15f7b32ae8b2d4028a4800`

## API

- `GET /api/v1/decision-cases/{caseId}`
- `GET /api/v1/decision-cases/by-application/{applicationId}`

## Events

- Consumes: `loan.application.submitted.v1`
- Publishes: `governance.review.started.v1`, `governance.agent-result.validated.v2`

Phase 2 does not emit `loan.decision.commanded.v1` from Agent proposals. Final Loan state changes remain outside this service path.

## Environment

- `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CONSUMER_GROUP`
- `GOVERNANCE_KAFKA_ENABLED`
- `TOPIC_LOAN_APPLICATION_SUBMITTED`
- `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE_SECONDS`, `OUTBOX_INSTANCE_ID`
- `CONTRACTS_ROOT`
- `LOAN_SERVICE_BASE_URL`, `LOAN_SERVICE_INTERNAL_TOKEN`
- `LOAN_SERVICE_CONNECT_TIMEOUT`, `LOAN_SERVICE_RESPONSE_TIMEOUT`
- `AGENT_RUNTIME_ENABLED`, `AGENT_RUNTIME_BASE_URL`
- `AGENT_RUNTIME_CONNECT_TIMEOUT`, `AGENT_RUNTIME_RESPONSE_TIMEOUT`
- `AGENT_RUNTIME_MAX_ATTEMPTS`, `AGENT_RUNTIME_REQUEST_TIMEOUT`
- `PHASE2_EXECUTION_PLAN_VERSION`
- `PHASE2_FEATURE_SCHEMA_VERSION`, `PHASE2_PREPROCESSING_VERSION`
- `PHASE2_MODEL_VERSION`, `PHASE2_MODEL_ARTIFACT_DIGEST`, `PHASE2_THRESHOLD_VERSION`

## Run

```bash
make test
make package
cp .env.example .env
# Fill .env with local secret values.
make run-local
make build-image
```

`make build-image` packages the service and builds
`rippleguard-governance-service:<commit-sha-12>`. The image records
`org.opencontainers.image.revision` as the full Git commit SHA and
`org.opencontainers.image.source` as this repository URL. After this PR is
merged, build the final Governance Service image from the new `main` merge
commit in this repository. RippleGuard Infra records and verifies the immutable
image baseline; Infra does not own the Governance image build.

Phase 1 mock evaluation remains documented as historical context only. The production Phase 2 path does not fall back to mock evaluation, prior proposals, default models, or automatic Loan decision commands.

## Docs

- [Architecture](docs/architecture.md)
- [Domain Model](docs/domain-model.md)
- [Mock Evaluation](docs/mock-evaluation.md)
- [Phase 2 Agent Orchestration](docs/phase-2-agent-orchestration.md)
- [Event Flow](docs/event-flow.md)
- [Testing](docs/testing.md)
- [Troubleshooting](docs/troubleshooting.md)
