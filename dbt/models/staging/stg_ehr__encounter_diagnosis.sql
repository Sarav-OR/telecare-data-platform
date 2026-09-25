select
    {{ hash_key(['encounter_id']) }}                    as hk_encounter,
    {{ hashdiff(['icd10_code', 'diagnosis_role', 'is_deleted']) }} as hd_encounter_diagnosis,
    encounter_id,
    seq_no,
    icd10_code,
    diagnosis_role,
    is_deleted,
    modified_at                                         as applied_datetime,
    ingested_at                                         as load_datetime,
    'EHR.encounter_diagnosis'                           as record_source
from {{ source('silver', 'ehr_encounter_diagnosis') }}
