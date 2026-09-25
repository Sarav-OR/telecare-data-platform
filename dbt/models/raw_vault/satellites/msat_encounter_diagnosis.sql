-- Multi-active satellite: several diagnoses per encounter (dependent key seq_no)
{{ vault_sat(source='stg_ehr__encounter_diagnosis', parent_hash_key='hk_encounter',
             hashdiff_column='hd_encounter_diagnosis', dependent_key='seq_no',
             payload=['encounter_id', 'icd10_code', 'diagnosis_role', 'is_deleted']) }}
