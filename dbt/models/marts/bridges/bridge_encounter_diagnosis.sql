-- Many diagnoses per encounter. weighting_factor sums to 1 per encounter, so measures
-- allocated across diagnoses are not double-counted.
with current_diag as (
    select d.encounter_id, d.seq_no, d.icd10_code, d.diagnosis_role, d.is_deleted
    from {{ ref('msat_encounter_diagnosis') }} d
    qualify row_number() over (partition by d.hk_encounter, d.seq_no order by d.applied_datetime desc) = 1
)
select
    cd.encounter_id                                                  as encounter_key,
    cd.seq_no,
    coalesce(dg.diagnosis_key, '{{ var("unknown_key") }}')           as diagnosis_key,
    cd.icd10_code                                                    as source_icd10_code,
    cd.diagnosis_role,
    1.0 / count(*) over (partition by cd.encounter_id)               as weighting_factor
from current_diag cd
left join {{ ref('dim_diagnosis') }} dg on dg.diagnosis_key = cd.icd10_code
where not cd.is_deleted
