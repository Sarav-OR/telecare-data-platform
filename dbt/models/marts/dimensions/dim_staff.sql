with versions as (
    {{ scd2_from_sat(ref('sat_staff'), 'hk_staff') }}
)
select
    {{ scd2_sk('hk_staff') }}                                      as staff_sk,
    hk_staff, staff_id, staff_login, role, specialty, team, languages, employment_pct,
    hired_at, left_at, is_deleted, valid_from, valid_to, is_current
from versions
union all
select '{{ var("unknown_key") }}', null, 'UNKNOWN', null, 'Unknown', null, null, null, null, null, null, false,
       cast('1900-01-01 00:00:00' as timestamp), {{ high_date() }}, true
