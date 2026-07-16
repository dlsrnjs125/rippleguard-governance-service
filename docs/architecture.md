# Architecture

The service owns an independent Governance database and never reads Loan Service tables.

Main components:

- Kafka consumer for `loan.application.submitted.v1`
- Application service that creates Decision Case and Evaluation Run records
- Deterministic Phase 1 mock evaluator and mock assurance
- Transactional outbox for Governance and Loan command events
- REST read API for Decision Case lookup

Baselines:

- Contracts: `dlsrnjs125/rippleguard-contracts@29f6c348fd93633476438ee36b3f93a3d036e165`
- Loan Service: `dlsrnjs125/rippleguard-loan-service@54ea344a682723d61d9beedf4ade56ee48029c0d`
