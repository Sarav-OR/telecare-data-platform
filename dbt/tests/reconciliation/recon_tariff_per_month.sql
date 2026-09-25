-- Migration check: revenue (tariff CHF) per month is identical in vault and Kimball.
with vault as (
    select encounter_id, started_at, tariff_amount_chf
    from {{ ref('sat_encounter') }}
    qualify row_number() over (partition by hk_encounter order by applied_datetime desc) = 1
        and not is_deleted
),
v as (
    select {{ date_key(to_local_ts('started_at')) }} / 100 as year_month, sum(tariff_amount_chf) as chf
    from vault group by 1
),
k as (
    select date_key / 100 as year_month, sum(tariff_amount_chf) as chf
    from {{ ref('fact_encounter') }} where not is_deleted group by 1
)
select coalesce(v.year_month, k.year_month) as year_month, v.chf as vault_chf, k.chf as kimball_chf
from v full outer join k on k.year_month = v.year_month
where coalesce(v.chf, 0) <> coalesce(k.chf, 0)
