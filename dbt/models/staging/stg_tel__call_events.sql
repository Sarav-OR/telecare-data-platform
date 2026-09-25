-- S1 telephony events. The caller is identified only by a salted phone hash:
-- business-key collision code 'TEL' keeps it apart from EHR patient ids in hub_patient.
select
    {{ hash_key(['event_id']) }}                        as hk_call_event,
    {{ hash_key(['call_id']) }}                         as hk_call,
    {{ hash_key(["'TEL'", 'caller_phone_hash']) }}      as hk_patient,
    'TEL'                                               as patient_bkcc,
    caller_phone_hash                                   as patient_bk,
    event_id,
    call_id,
    sequence_no,
    event_type,
    event_ts,
    dialled_line,
    queue_code,
    agent_login,
    caller_phone_hash,
    ivr_language,
    dq_warnings,
    ingested_at                                         as load_datetime,
    'TEL.call_events'                                   as record_source
from {{ source('silver', 'call_events') }}
