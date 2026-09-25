-- Business rule: resolve TEL / APP identities to EHR master patients (current, not deleted).
-- A phone shared by a family maps to several patients -> is_shared_identity; contacts from
-- shared identities are attributed via the encounter, not via the phone.
with current_links as (
    select s.hk_sal_patient, s.identifier_type, s.identifier_value, s.patient_id, s.verified_at, s.is_deleted
    from {{ ref('sat_patient_identifier') }} s
    qualify row_number() over (partition by s.hk_sal_patient order by s.applied_datetime desc) = 1
),

resolved as (
    select
        l.hk_patient                                     as hk_identity,
        l.hk_patient_master,
        c.identifier_type,
        c.identifier_value,
        c.patient_id,
        c.verified_at
    from {{ ref('sal_patient') }} l
    join current_links c on c.hk_sal_patient = l.hk_sal_patient
    where not coalesce(c.is_deleted, false)
)

select
    *,
    count(*) over (partition by hk_identity) > 1        as is_shared_identity
from resolved
