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

- Contracts: `dlsrnjs125/rippleguard-contracts@5781bd30f688c25ae0d531049d6d7fb39ec3e9b1`
- Loan Service: `dlsrnjs125/rippleguard-loan-service@1f78f8c3358fc0437b15f7b32ae8b2d4028a4800`

The Phase 2 production path does not execute the historical mock evaluator and does not emit `loan.decision.commanded.v1` from an Agent proposal.
