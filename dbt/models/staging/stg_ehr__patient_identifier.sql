-- Verified identities of a patient in other systems -> same-as link.
-- Each row also feeds hub_patient with the TEL / APP business key.
with src as (
    select *,
           case identifier_type when 'PHONE_HASH' then 'TEL' when 'APP_USER_ID' then 'APP' end as identity_bkcc
    from {{ source('silver', 'ehr_patient_identifier') }}
)
select
    {{ hash_key(['identity_bkcc', 'identifier_value']) }}                    as hk_patient,           -- the identity
    identity_bkcc                                                            as patient_bkcc,
    identifier_value                                                         as patient_bk,
    {{ hash_key(["'EHR'", 'patient_id']) }}                                  as hk_patient_master,    -- the EHR patient
    {{ hash_key(['identity_bkcc', 'identifier_value', "'EHR'", 'patient_id']) }} as hk_sal_patient,
    {{ hashdiff(['identifier_id', 'verified_at', 'is_deleted']) }}           as hd_patient_identifier,
    identifier_id,
    identifier_type,
    identifier_value,
    patient_id,
    verified_at,
    is_deleted,
    modified_at                                                              as applied_datetime,
    ingested_at                                                              as load_datetime,
    'EHR.patient_identifier'                                                 as record_source
from src
