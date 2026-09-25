{%- set payload = ['channel', 'service_line', 'contact_system', 'contact_ref', 'started_at', 'ended_at',
                   'triage_level', 'disposition_code', 'plan_code', 'tariff_amount_chf', 'status', 'is_deleted'] %}
select
    {{ hash_key(['encounter_id']) }}                                   as hk_encounter,
    {{ hash_key(["'EHR'", 'patient_id']) }}                            as hk_patient,
    {{ hash_key(['staff_id']) }}                                       as hk_staff,
    {{ hash_key(['encounter_id', "'EHR'", 'patient_id', 'staff_id']) }} as hk_link_encounter,
    case when contact_system = 'TEL' then {{ hash_key(['contact_ref']) }} end   as hk_call,
    case when contact_system = 'APP' then {{ hash_key(['contact_ref']) }} end   as hk_app_session,
    case when contact_system = 'TEL' then {{ hash_key(['encounter_id', 'contact_ref']) }} end as hk_link_encounter_call,
    case when contact_system = 'APP' then {{ hash_key(['encounter_id', 'contact_ref']) }} end as hk_link_encounter_app,
    {{ hashdiff(payload) }}                                            as hd_encounter,
    encounter_id,
    patient_id,
    staff_id,
    {{ payload | join(',\n    ') }},
    modified_at                                                        as applied_datetime,
    ingested_at                                                        as load_datetime,
    'EHR.encounter'                                                    as record_source
from {{ source('silver', 'ehr_encounter') }}
