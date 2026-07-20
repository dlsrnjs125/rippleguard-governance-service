# Testing

Coverage includes:

- submitted event creates case, evaluation run, and decision command outbox
- duplicate submitted event idempotency
- deterministic mock reproducibility
- state transition coverage
- unsupported schema rejection
- missing snapshot reference to `VERIFICATION_REQUIRED`
- mock assurance violated to `BLOCKED`
- conflicting submitted event to `RECALCULATION_REQUIRED`
- unsupported version quarantine
- PostgreSQL Flyway validation
- PostgreSQL outbox claim query
- strict Governance event timestamp and causation ordering
- PostgreSQL outbox claim order for Governance event flow
- duplicate event race behavior

Run:

```bash
./mvnw test
```
