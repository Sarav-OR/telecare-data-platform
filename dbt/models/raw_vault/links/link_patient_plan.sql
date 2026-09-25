-- Patient insured under a plan; periods live in effsat_patient_coverage
{{ vault_link(source='stg_crm__patient_coverage', link_hash_key='hk_patient_plan',
              foreign_hash_keys=['hk_patient', 'hk_insurance_plan']) }}
