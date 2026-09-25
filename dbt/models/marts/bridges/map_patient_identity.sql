-- Which phone hash / app user id belongs to which patient (current patient version).
select
    i.identifier_type,
    i.identifier_value,
    i.patient_id,
    p.patient_sk                                                     as current_patient_sk,
    i.verified_at                                                    as verified_at_utc,
    i.is_shared_identity
from {{ ref('bv_patient_identity') }} i
left join {{ ref('dim_patient') }} p on p.hk_patient = i.hk_patient_master and p.is_current
