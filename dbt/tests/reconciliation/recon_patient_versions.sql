-- Migration check: every satellite version is exactly one SCD2 row, and the current
-- dimension row carries the latest satellite attributes.
with sat as (
    select patient_id, applied_datetime, canton, preferred_language,
           row_number() over (partition by hk_patient order by applied_datetime desc) as rn
    from {{ ref('sat_patient_ehr') }}
),
counts as (
    select 'version_count' as check_name,
           (select count(*) from sat) as vault_value,
           (select count(*) from {{ ref('dim_patient') }} where patient_sk <> '{{ var("unknown_key") }}') as kimball_value
),
current_mismatch as (
    select 'current_attributes' as check_name, count(*) as vault_value, 0 as kimball_value
    from sat s
    join {{ ref('dim_patient') }} d on d.patient_id = s.patient_id and d.is_current
    where s.rn = 1 and (s.canton <> d.canton_code or s.preferred_language <> d.preferred_language)
)
select * from counts where vault_value <> kimball_value
union all
select * from current_mismatch where vault_value > 0
