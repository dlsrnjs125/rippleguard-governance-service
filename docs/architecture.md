# Architecture

The service owns an independent Governance database and never reads Loan Service tables.

Main components:

- Kafka consumer for `loan.application.submitted.v1`
- Application service that creates Decision Case and Evaluation Run records
- Phase 2 Loan Decision Agent Runtime client
- Contracts-backed Agent Request, Agent Result, and Governance validation event verification
- Transactional outbox for Governance validation audit events
- REST read API for Decision Case lookup

Baselines:

- Contracts: `dlsrnjs125/rippleguard-contracts@f4012e8`
- Loan Service: `dlsrnjs125/rippleguard-loan-service@54ea344a682723d61d9beedf4ade56ee48029c0d`

The Phase 2 production path does not execute the historical mock evaluator and does not emit `loan.decision.commanded.v1` from an Agent proposal.
