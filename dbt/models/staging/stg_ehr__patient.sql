{%- set payload = ['date_of_birth', 'sex', 'canton', 'postal_code', 'preferred_language', 'is_deleted'] %}
-- EHR row versions: modified_at is the moment the change happened in the source
-- (applied time); ingested_at is when the platform received it (load time).
select
    {{ hash_key(["'EHR'", 'patient_id']) }}             as hk_patient,
    'EHR'                                               as patient_bkcc,
    patient_id                                          as patient_bk,
    {{ hashdiff(payload) }}                             as hd_patient,
    patient_id,
    {{ payload | join(',\n    ') }},
    modified_at                                         as applied_datetime,
    ingested_at                                         as load_datetime,
    'EHR.patient'                                       as record_source
from {{ source('silver', 'ehr_patient') }}
