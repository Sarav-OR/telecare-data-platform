-- S2 app events. Test/QA accounts stay in the raw vault (it stores everything) and
-- are excluded in the business vault.
select
    {{ hash_key(['event_id']) }}                        as hk_app_event,
    {{ hash_key(['session_id']) }}                      as hk_app_session,
    {{ hash_key(["'APP'", 'app_user_id']) }}            as hk_patient,
    'APP'                                               as patient_bkcc,
    app_user_id                                         as patient_bk,
    event_id,
    session_id,
    app_user_id,
    event_name,
    client_ts,
    server_ts,
    platform,
    app_version,
    service_line,
    channel,
    symptom_category,
    triage_score,
    wait_estimate_min,
    is_test_account,
    dq_warnings,
    ingested_at                                         as load_datetime,
    'APP.app_events'                                    as record_source
from {{ source('silver', 'app_events') }}
