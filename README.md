# RippleGuard Governance Service

Phase 1 Governance Core service. It owns Decision Case and Evaluation Run state, consumes Loan application submissions, runs deterministic mock evaluation/assurance, and emits command events through a transactional outbox.

## Baselines

- Contracts: `dlsrnjs125/rippleguard-contracts@29f6c348fd93633476438ee36b3f93a3d036e165`
- Loan Service: `dlsrnjs125/rippleguard-loan-service@54ea344a682723d61d9beedf4ade56ee48029c0d`

## API

- `GET /api/v1/decision-cases/{caseId}`
- `GET /api/v1/decision-cases/by-application/{applicationId}`

## Events

- Consumes: `loan.application.submitted.v1`
- Publishes: `governance.review.started.v1`, `agent.evaluation.requested.v1`, `agent.evaluation.completed.v1`, `loan.decision.commanded.v1`

## Environment

- `DB_URL`, `DB_USERNAME`, `DB_PASSWORD`
- `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_CONSUMER_GROUP`
- `GOVERNANCE_KAFKA_ENABLED`
- `TOPIC_LOAN_APPLICATION_SUBMITTED`
- `OUTBOX_BATCH_SIZE`, `OUTBOX_LEASE_SECONDS`, `OUTBOX_INSTANCE_ID`

## Run

```bash
./mvnw test
./mvnw package
docker build -t rippleguard-governance-service:local .
```

Mock evaluation is deterministic and not a real financial credit policy. Phase 1 executes mock requested/completed/assurance inside the service transaction for trace contract coverage; Agent Runtime integration is deferred.

## Docs

- [Architecture](docs/architecture.md)
- [Domain Model](docs/domain-model.md)
- [Mock Evaluation](docs/mock-evaluation.md)
- [Event Flow](docs/event-flow.md)
- [Testing](docs/testing.md)
- [Troubleshooting](docs/troubleshooting.md)
