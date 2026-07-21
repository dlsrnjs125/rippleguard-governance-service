alter table evaluation_run add column agent_run_id uuid;
alter table evaluation_run add column request_idempotency_key varchar(160);
alter table evaluation_run add column snapshot_id varchar(128);
alter table evaluation_run add column snapshot_schema_version varchar(64);
alter table evaluation_run add column snapshot_digest varchar(80);
alter table evaluation_run add column feature_schema_version varchar(128);
alter table evaluation_run add column preprocessing_version varchar(128);
alter table evaluation_run add column model_version varchar(128);
alter table evaluation_run add column model_artifact_digest varchar(80);
alter table evaluation_run add column threshold_version varchar(128);
alter table evaluation_run add column attempt_count integer not null default 0;
alter table evaluation_run add column max_attempts integer not null default 1;
alter table evaluation_run add column requested_at timestamp with time zone;
alter table evaluation_run add column deadline_at timestamp with time zone;
alter table evaluation_run add column failure_classification varchar(64);
alter table evaluation_run add column failure_reason_code varchar(128);
alter table evaluation_run add column accepted_result_digest varchar(80);
alter table evaluation_run add column accepted_proposal_snapshot text;
alter table evaluation_run add column validation_outcome varchar(32);
alter table evaluation_run add column source_event_id uuid;
alter table evaluation_run add column snapshot_created_at timestamp with time zone;
alter table evaluation_run add column snapshot_reference text;
alter table evaluation_run add column reference_type varchar(64);
alter table evaluation_run add column last_attempt_started_at timestamp with time zone;
alter table evaluation_run add column next_attempt_at timestamp with time zone;
alter table evaluation_run add column lease_owner varchar(128);
alter table evaluation_run add column lease_until timestamp with time zone;
alter table evaluation_run add column last_transport_failure_code varchar(128);

create unique index uq_evaluation_run_agent_run_id
    on evaluation_run(agent_run_id);

create unique index uq_evaluation_run_request_idempotency_key
    on evaluation_run(request_idempotency_key);

alter table evaluation_run add constraint ck_evaluation_run_validation_outcome
    check (validation_outcome is null or validation_outcome in ('VALIDATED', 'REJECTED'));

alter table evaluation_run add constraint ck_evaluation_run_attempt_count
    check (attempt_count >= 0);

alter table evaluation_run add constraint ck_evaluation_run_max_attempts
    check (max_attempts >= 1);
