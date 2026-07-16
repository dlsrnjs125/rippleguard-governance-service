# RippleGuard Governance Service

Phase 1 Governance Core service.

## Run

```bash
../rippleguard-loan-service/mvnw -f pom.xml test
../rippleguard-loan-service/mvnw -f pom.xml package
docker build -t rippleguard-governance-service:local .
```

## Docs

- [Architecture](docs/architecture.md)
- [Domain Model](docs/domain-model.md)
- [Mock Evaluation](docs/mock-evaluation.md)
- [Event Flow](docs/event-flow.md)
- [Testing](docs/testing.md)
- [Troubleshooting](docs/troubleshooting.md)
