-- SCD2 on the plan; insurer name as type 1 (current value)
with versions as (
    {{ scd2_from_sat(ref('sat_insurance_plan'), 'hk_insurance_plan') }}
),
insurer as (
    select insurer_id, insurer_name
    from {{ ref('sat_insurer') }}
    qualify row_number() over (partition by hk_insurer order by applied_datetime desc) = 1
)
select
    {{ scd2_sk('v.hk_insurance_plan') }}                           as plan_sk,
    v.hk_insurance_plan, v.plan_code, v.plan_name, v.plan_model, v.telmed_first_contact_required,
    v.premium_discount_pct, v.insurer_id, i.insurer_name, v.valid_from, v.valid_to, v.is_current
from versions v
left join insurer i on i.insurer_id = v.insurer_id
union all
select '{{ var("unknown_key") }}', null, 'UNKNOWN', 'Unknown', null, null, null, null, 'Unknown',
       cast('1900-01-01 00:00:00' as timestamp), {{ high_date() }}, true
