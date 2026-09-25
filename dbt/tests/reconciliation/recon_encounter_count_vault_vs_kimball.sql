-- Migration check: every encounter in the vault exists exactly once in the Kimball fact,
-- and soft-deleted encounters are flagged identically in both.
with vault as (
    select encounter_id, is_deleted
    from {{ ref('sat_encounter') }}
    qualify row_number() over (partition by hk_encounter order by applied_datetime desc) = 1
),
kimball as (select encounter_key, is_deleted from {{ ref('fact_encounter') }})
select coalesce(v.encounter_id, k.encounter_key) as encounter_id, v.is_deleted as vault_deleted, k.is_deleted as fact_deleted
from vault v
full outer join kimball k on k.encounter_key = v.encounter_id
where v.encounter_id is null or k.encounter_key is null or v.is_deleted <> k.is_deleted
