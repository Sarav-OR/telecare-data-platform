-- SCD2 patient dimension. No date of birth (PII): ages are computed per fact event.
{{ config(liquid_clustered_by=['patient_id']) }}
with versions as (
    {{ scd2_from_sat(ref('sat_patient_ehr'), 'hk_patient') }}
)
select
    {{ scd2_sk('v.hk_patient') }}                                  as patient_sk,
    v.hk_patient,
    v.patient_id,
    v.sex,
    v.canton                                                       as canton_code,
    c.canton_name,
    c.language_region,
    v.preferred_language,
    v.is_deleted,
    v.valid_from,
    v.valid_to,
    v.is_current
from versions v
left join {{ ref('cantons') }} c on c.canton_code = v.canton
union all
select '{{ var("unknown_key") }}', null, 'UNKNOWN', null, null, 'Unknown', null, null, false,
       cast('1900-01-01 00:00:00' as timestamp), {{ high_date() }}, true
