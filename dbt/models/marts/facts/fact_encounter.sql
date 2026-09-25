{#
  fact_encounter - one row per teleconsultation (accumulating snapshot).

  Incremental, change-driven: an encounter is (re)built when anything about it was
  loaded within the lookback window - the encounter itself, a late diagnosis
  correction, a prescription, a referral or a sick note. Its row is then rebuilt
  completely and MERGEd, so late-arriving clinical coding updates the fact in place.
  Dimension keys are point-in-time: the patient / clinician / plan version valid when
  the encounter started.
#}
{{ config(unique_key='encounter_key', liquid_clustered_by=['date_key']) }}

with changed as (
    {% if is_incremental() %}
    select hk_encounter from {{ ref('sat_encounter') }} where {{ lookback_filter('load_datetime') }}
    union
    select hk_encounter from {{ ref('msat_encounter_diagnosis') }} where {{ lookback_filter('load_datetime') }}
    union
    select l.hk_encounter from {{ ref('link_prescription') }} l
    join {{ ref('sat_prescription') }} s on s.hk_prescription = l.hk_prescription
    where {{ lookback_filter('s.load_datetime') }}
    union
    select l.hk_encounter from {{ ref('link_referral') }} l
    join {{ ref('sat_referral') }} s on s.hk_referral = l.hk_referral
    where {{ lookback_filter('s.load_datetime') }}
    union
    select l.hk_encounter from {{ ref('link_sick_note') }} l
    join {{ ref('sat_sick_note') }} s on s.hk_sick_note = l.hk_sick_note
    where {{ lookback_filter('s.load_datetime') }}
    {% else %}
    select hk_encounter from {{ ref('hub_encounter') }}
    {% endif %}
),

enc as (   -- current state of each changed encounter
    select s.*
    from {{ ref('sat_encounter') }} s
    join changed c on c.hk_encounter = s.hk_encounter
    qualify row_number() over (partition by s.hk_encounter order by s.applied_datetime desc) = 1
),

diag as (
    select d.hk_encounter, d.seq_no, d.icd10_code
    from {{ ref('msat_encounter_diagnosis') }} d
    join changed c on c.hk_encounter = d.hk_encounter
    qualify row_number() over (partition by d.hk_encounter, d.seq_no order by d.applied_datetime desc) = 1
        and not d.is_deleted
),

diag_agg as (
    select hk_encounter,
           count(*)                                                        as diagnosis_count,
           max(case when seq_no = 1 then icd10_code end)                   as primary_icd10_code
    from diag group by hk_encounter
),

rx as (
    select l.hk_encounter, s.atc_code
    from {{ ref('link_prescription') }} l
    join changed c on c.hk_encounter = l.hk_encounter
    join {{ ref('sat_prescription') }} s on s.hk_prescription = l.hk_prescription
    qualify row_number() over (partition by s.hk_prescription order by s.applied_datetime desc) = 1
        and not s.is_deleted
),

rx_agg as (
    select rx.hk_encounter,
           count(*)                                                        as prescription_count,
           max(case when m.is_antibiotic then 1 else 0 end) = 1            as has_antibiotic
    from rx left join {{ ref('atc_codes') }} m on m.atc_code = rx.atc_code
    group by rx.hk_encounter
),

ref_agg as (
    select l.hk_encounter, max(l.hk_partner) as hk_referral_partner, count(*) as referral_count
    from {{ ref('link_referral') }} l
    join changed c on c.hk_encounter = l.hk_encounter
    join {{ ref('sat_referral') }} s on s.hk_referral = l.hk_referral
    where not s.is_deleted
    group by l.hk_encounter
),

sick as (
    select l.hk_encounter, max(s.days) as sick_note_days
    from {{ ref('link_sick_note') }} l
    join changed c on c.hk_encounter = l.hk_encounter
    join {{ ref('sat_sick_note') }} s on s.hk_sick_note = l.hk_sick_note
    where not s.is_deleted
    group by l.hk_encounter
),

contact as (   -- when did the patient first reach out (for wait time)
    select 'TEL' as contact_system, call_id as contact_ref, contact_started_at from {{ ref('bv_call_session') }}
    union all
    select 'APP', session_id, contact_started_at from {{ ref('bv_app_session') }}
),

dob as (
    select hk_patient, date_of_birth
    from {{ ref('sat_patient_ehr') }}
    qualify row_number() over (partition by hk_patient order by applied_datetime desc) = 1
),

base as (
    select
        e.*,
        {{ hash_key(["'EHR'", 'e.patient_id']) }}                      as hk_patient,
        {{ hash_key(['e.staff_id']) }}                                  as hk_staff,
        {{ hash_key(['e.plan_code']) }}                                 as hk_insurance_plan,
        {{ to_local_ts('e.started_at') }}                               as started_at_local
    from enc e          -- soft-deleted encounters are kept and flagged, so MERGE can retire them
)

select
    b.encounter_id                                                      as encounter_key,
    {{ date_key('b.started_at_local') }}                                as date_key,
    {{ time_key('b.started_at_local') }}                                as time_key,
    coalesce(p.patient_sk, '{{ var("unknown_key") }}')                  as patient_sk,
    coalesce(st.staff_sk, '{{ var("unknown_key") }}')                   as staff_sk,
    coalesce(pl.plan_sk, '{{ var("unknown_key") }}')                    as plan_sk,
    coalesce(pa.partner_sk, '{{ var("unknown_key") }}')                 as referral_partner_sk,
    coalesce(ch.channel_key, '{{ var("unknown_key") }}')                as channel_key,
    coalesce(sl.service_line_key, '{{ var("unknown_key") }}')           as service_line_key,
    coalesce(tl.triage_level_key, '{{ var("unknown_key") }}')           as triage_level_key,
    coalesce(di.disposition_key, '{{ var("unknown_key") }}')            as disposition_key,
    coalesce(dg.diagnosis_key, '{{ var("unknown_key") }}')              as primary_diagnosis_key,
    b.contact_system,
    b.contact_ref,
    b.status,
    b.is_deleted,
    b.started_at                                                        as started_at_utc,
    b.ended_at                                                          as ended_at_utc,
    b.started_at_local,
    -- measures
    {{ seconds_between('c.contact_started_at', 'b.started_at') }}       as wait_seconds,
    {{ seconds_between('b.started_at', 'b.ended_at') }}                 as consultation_seconds,
    b.tariff_amount_chf,
    {{ age_at('dob.date_of_birth', 'cast(b.started_at_local as date)') }} as age_at_encounter,
    {{ age_band(age_at('dob.date_of_birth', 'cast(b.started_at_local as date)')) }} as age_band,
    coalesce(da.diagnosis_count, 0)                                     as diagnosis_count,
    coalesce(ra.prescription_count, 0)                                  as prescription_count,
    coalesce(rf.referral_count, 0)                                      as referral_count,
    -- flags
    coalesce(ra.prescription_count, 0) > 0                              as has_prescription,
    coalesce(ra.has_antibiotic, false)                                  as has_antibiotic,
    coalesce(rf.referral_count, 0) > 0                                  as has_referral,
    sk.sick_note_days is not null                                       as has_sick_note,
    coalesce(di.is_resolved_remotely, false) and coalesce(rf.referral_count, 0) = 0 as is_resolved_remotely,
    b.disposition_code in ('EMERGENCY_DEPT', 'AMBULANCE')               as is_emergency_escalation,
    coalesce(td.is_night_tariff, false)                                 as is_night,
    coalesce(dd.is_weekend_or_holiday, false)                           as is_weekend_or_holiday,
    b.load_datetime                                                     as _loaded_at
from base b
left join contact c                        on c.contact_system = b.contact_system and c.contact_ref = b.contact_ref
left join dob                              on dob.hk_patient = b.hk_patient
left join diag_agg da                      on da.hk_encounter = b.hk_encounter
left join rx_agg ra                        on ra.hk_encounter = b.hk_encounter
left join ref_agg rf                       on rf.hk_encounter = b.hk_encounter
left join sick sk                          on sk.hk_encounter = b.hk_encounter
left join {{ ref('dim_patient') }} p       on p.hk_patient = b.hk_patient and {{ pit('p', 'b.started_at') }}
left join {{ ref('dim_staff') }} st        on st.hk_staff = b.hk_staff and {{ pit('st', 'b.started_at') }}
left join {{ ref('dim_insurance_plan') }} pl on pl.hk_insurance_plan = b.hk_insurance_plan and {{ pit('pl', 'b.started_at') }}
left join {{ ref('dim_partner') }} pa      on pa.hk_partner = rf.hk_referral_partner and {{ pit('pa', 'b.started_at') }}
left join {{ ref('dim_channel') }} ch      on ch.channel_key = b.channel
left join {{ ref('dim_service_line') }} sl on sl.service_line_key = b.service_line
left join {{ ref('dim_triage_level') }} tl on tl.triage_level_key = cast(b.triage_level as {{ dbt.type_string() }})
left join {{ ref('dim_disposition') }} di  on di.disposition_key = b.disposition_code
left join {{ ref('dim_diagnosis') }} dg    on dg.diagnosis_key = da.primary_icd10_code
left join {{ ref('dim_time_of_day') }} td  on td.time_key = {{ time_key('b.started_at_local') }}
left join {{ ref('dim_date') }} dd         on dd.date_key = {{ date_key('b.started_at_local') }}
