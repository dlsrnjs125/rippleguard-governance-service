create table decision_case (
    case_id varchar(128) primary key,
    application_id uuid not null unique,
    applicant_id varchar(255) not null,
    input_snapshot_version varchar(64) not null,
    status varchar(64) not null,
    final_decision varchar(32),
    assurance_result varchar(64),
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    version bigint not null,
    constraint ck_decision_case_status check (status in (
        'REVIEW_STARTED',
        'EVALUATION_REQUESTED',
        'EVALUATION_COMPLETED',
        'DECISION_COMMANDED',
        'FAILED'
    )),
    constraint ck_decision_case_final check (final_decision is null or final_decision in ('APPROVE', 'REJECT'))
);

create table evaluation_run (
    evaluation_run_id uuid primary key,
    case_id varchar(128) not null references decision_case(case_id),
    rule_version varchar(64) not null,
    input_snapshot_version varchar(64) not null,
    decision_id uuid not null unique,
    status varchar(32) not null,
    proposal varchar(32),
    confidence numeric(5,4),
    reason_codes text,
    requested_at timestamp with time zone not null,
    completed_at timestamp with time zone,
    version bigint not null,
    constraint uq_evaluation_run_case unique (case_id),
    constraint ck_evaluation_run_status check (status in ('REQUESTED', 'COMPLETED', 'FAILED')),
    constraint ck_evaluation_run_proposal check (proposal is null or proposal in ('APPROVE', 'REJECT')),
    constraint ck_evaluation_run_confidence check (confidence is null or (confidence >= 0 and confidence <= 1))
);

create table outbox_event (
    event_id uuid primary key,
    event_type varchar(128) not null,
    schema_version varchar(32) not null,
    aggregate_id uuid not null,
    correlation_id varchar(128) not null,
    causation_id uuid,
    payload jsonb not null,
    status varchar(32) not null,
    attempts integer not null,
    next_attempt_at timestamp with time zone not null,
    processing_started_at timestamp with time zone,
    lease_until timestamp with time zone,
    claimed_by varchar(128),
    claim_token uuid,
    published_at timestamp with time zone,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null,
    constraint ck_outbox_event_status check (status in ('PENDING', 'PROCESSING', 'PUBLISHED', 'FAILED')),
    constraint ck_outbox_event_attempts check (attempts >= 0)
);

create index ix_outbox_status_next_attempt on outbox_event(status, next_attempt_at);
create index ix_outbox_processing_lease on outbox_event(status, lease_until);

create table inbox_event (
    event_id uuid primary key,
    event_type varchar(128) not null,
    application_id uuid,
    payload_hash varchar(64) not null,
    processed_at timestamp with time zone not null
);
