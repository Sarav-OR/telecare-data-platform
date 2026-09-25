{%- set payload = ['plan_name', 'plan_model', 'telmed_first_contact_required', 'premium_discount_pct'] %}
select
    {{ hash_key(['plan_code']) }}                       as hk_insurance_plan,
    {{ hash_key(['insurer_id']) }}                      as hk_insurer,
    {{ hash_key(['plan_code', 'insurer_id']) }}         as hk_plan_insurer,
    {{ hashdiff(payload) }}                             as hd_insurance_plan,
    plan_code,
    insurer_id,
    {{ payload | join(',\n    ') }},
    cast(snapshot_date as timestamp)                    as applied_datetime,
    ingested_at                                         as load_datetime,
    'CRM.insurance_plans'                               as record_source
from {{ source('silver', 'crm_insurance_plans') }}
