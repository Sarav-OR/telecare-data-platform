select
    {{ hash_key(["'EHR'", 'patient_id']) }}             as hk_patient,
    'EHR'                                               as patient_bkcc,
    patient_id                                          as patient_bk,
    {{ hash_key(['plan_code']) }}                       as hk_insurance_plan,
    {{ hash_key(["'EHR'", 'patient_id', 'plan_code']) }} as hk_patient_plan,
    {{ hashdiff(['valid_from', 'valid_to']) }}          as hd_coverage,
    coverage_id,
    patient_id,
    plan_code,
    valid_from,
    valid_to,
    cast(snapshot_date as timestamp)                    as applied_datetime,
    ingested_at                                         as load_datetime,
    'CRM.patient_coverage'                              as record_source
from {{ source('silver', 'crm_patient_coverage') }}
