alter table evaluation_run add column feature_payload_digest varchar(80);
alter table evaluation_run add column feature_payload text;
alter table evaluation_run add column request_event_id uuid;

create unique index uq_evaluation_run_request_event_id
    on evaluation_run(request_event_id);
