-- Migration check: active diagnoses in the multi-active satellite = rows in the bridge.
select * from (
    select
        (select count(*) from (
            select is_deleted from {{ ref('msat_encounter_diagnosis') }}
            qualify row_number() over (partition by hk_encounter, seq_no order by applied_datetime desc) = 1
        ) x where not is_deleted)                                      as vault_active_diagnoses,
        (select count(*) from {{ ref('bridge_encounter_diagnosis') }}) as bridge_rows
) t
where vault_active_diagnoses <> bridge_rows
