-- Effectivity satellite: coverage periods of patient-plan relationships (dependent key coverage_id)
{{ vault_sat(source='stg_crm__patient_coverage', parent_hash_key='hk_patient_plan', hashdiff_column='hd_coverage',
             dependent_key='coverage_id', payload=['patient_id', 'plan_code', 'valid_from', 'valid_to']) }}
