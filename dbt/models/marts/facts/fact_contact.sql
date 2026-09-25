{#
  fact_contact - one row per inbound contact (phone call or app session).
  Patient attribution: the patient of the resulting encounter if there is one,
  otherwise the verified identity - but only when that identity belongs to exactly
  one patient (a shared family phone is never guessed).
#}
{{ config(unique_key='contact_key', liquid_clustered_by=['date_key']) }}

with calls as (
    select
        call_id                                   as contact_key,
        'TEL'                                     as contact_system,
        hk_call                                   as hk_contact,
        hk_caller                                 as hk_identity,
        contact_started_at,
        'PHONE'                                   as channel_code,
        sl.service_line_code,
        queue_code,
        agent_login,
        contact_outcome_code,
        queue_seconds,
        handle_seconds,
        answered_at is not null                   as is_answered,
        is_answered_within_sl,
        contact_outcome_code = 'ABANDONED'        as is_abandoned,
        contact_outcome_code = 'CALLBACK'         as is_callback,
        last_loaded_at
    from {{ ref('bv_call_session') }} c
    left join {{ ref('service_lines') }} sl on sl.dialled_line = c.dialled_line
    {% if is_incremental() %}where {{ lookback_filter('last_loaded_at') }}{% endif %}
),

apps as (
    select
        session_id                                as contact_key,
        'APP'                                     as contact_system,
        hk_app_session                            as hk_contact,
        hk_app_user                               as hk_identity,
        contact_started_at,
        channel                                   as channel_code,
        service_line                              as service_line_code,
        cast(null as {{ dbt.type_string() }})     as queue_code,
        cast(null as {{ dbt.type_string() }})     as agent_login,
        contact_outcome_code,
        queue_seconds,
        handle_seconds,
        consultation_started_at is not null       as is_answered,
        cast(null as boolean)                     as is_answered_within_sl,
        contact_outcome_code = 'SESSION_CANCELLED' as is_abandoned,
        false                                     as is_callback,
        last_loaded_at
    from {{ ref('bv_app_session') }}
    {% if is_incremental() %}where {{ lookback_filter('last_loaded_at') }}{% endif %}
),

contacts as (select * from calls union all select * from apps),

encounter_links as (
    select hk_call as hk_contact, hk_encounter from {{ ref('link_encounter_call') }}
    union all
    select hk_app_session, hk_encounter from {{ ref('link_encounter_app_session') }}
),

first_encounter as (   -- a callback can lead to one encounter per contact
    select el.hk_contact, h.encounter_id, le.hk_patient
    from encounter_links el
    join {{ ref('hub_encounter') }} h on h.hk_encounter = el.hk_encounter
    join {{ ref('link_encounter') }} le on le.hk_encounter = el.hk_encounter
    qualify row_number() over (partition by el.hk_contact order by h.encounter_id) = 1
),

identity as (
    select hk_identity, max(hk_patient_master) as hk_patient
    from {{ ref('bv_patient_identity') }}
    where not is_shared_identity
    group by hk_identity
),

staff_login as (
    select staff_login, hk_staff
    from {{ ref('sat_staff') }}
    qualify row_number() over (partition by staff_login order by applied_datetime desc) = 1
),

resolved as (
    select
        c.*,
        {{ to_local_ts('c.contact_started_at') }}          as started_at_local,
        fe.encounter_id,
        coalesce(fe.hk_patient, i.hk_patient)             as hk_patient,
        case when fe.hk_patient is not null then 'ENCOUNTER'
             when i.hk_patient is not null then 'VERIFIED_IDENTITY'
             else 'UNRESOLVED' end                        as patient_resolution,
        sl.hk_staff
    from contacts c
    left join first_encounter fe on fe.hk_contact = c.hk_contact
    left join identity i on i.hk_identity = c.hk_identity
    left join staff_login sl on sl.staff_login = c.agent_login
)

select
    r.contact_key,
    {{ date_key('r.started_at_local') }}                    as date_key,
    {{ time_key('r.started_at_local') }}                    as time_key,
    coalesce(p.patient_sk, '{{ var("unknown_key") }}')      as patient_sk,
    coalesce(st.staff_sk, '{{ var("unknown_key") }}')       as agent_staff_sk,
    coalesce(ch.channel_key, '{{ var("unknown_key") }}')    as channel_key,
    coalesce(sv.service_line_key, '{{ var("unknown_key") }}') as service_line_key,
    coalesce(q.queue_key, '{{ var("unknown_key") }}')       as queue_key,
    coalesce(o.contact_outcome_key, '{{ var("unknown_key") }}') as contact_outcome_key,
    r.contact_system,
    r.encounter_id                                          as encounter_key,
    r.patient_resolution,
    r.contact_started_at                                    as started_at_utc,
    r.started_at_local,
    r.queue_seconds,
    r.handle_seconds,
    r.is_answered,
    r.is_answered_within_sl,
    r.is_abandoned,
    r.is_callback,
    r.encounter_id is not null                              as is_converted_to_encounter,
    r.last_loaded_at                                        as _loaded_at
from resolved r
left join {{ ref('dim_patient') }} p          on p.hk_patient = r.hk_patient and {{ pit('p', 'r.contact_started_at') }}
left join {{ ref('dim_staff') }} st           on st.hk_staff = r.hk_staff and {{ pit('st', 'r.contact_started_at') }}
left join {{ ref('dim_channel') }} ch         on ch.channel_key = r.channel_code
left join {{ ref('dim_service_line') }} sv    on sv.service_line_key = r.service_line_code
left join {{ ref('dim_queue') }} q            on q.queue_key = r.queue_code
left join {{ ref('dim_contact_outcome') }} o  on o.contact_outcome_key = r.contact_outcome_code
