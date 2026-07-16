# Testing

Coverage includes:

- submitted event creates case, evaluation run, and decision command outbox
- duplicate submitted event idempotency
- deterministic mock reproducibility
- state transition coverage
- unsupported schema rejection
- missing snapshot reference rejection
- PostgreSQL Flyway validation
- PostgreSQL outbox claim query
- duplicate event race behavior

Run:

```bash
../rippleguard-loan-service/mvnw -f pom.xml test
```
