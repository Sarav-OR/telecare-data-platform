-- Verification status of a same-as relationship (soft deletes end the relationship)
{{ vault_sat(source='stg_ehr__patient_identifier', parent_hash_key='hk_sal_patient',
             hashdiff_column='hd_patient_identifier',
             payload=['identifier_id', 'identifier_type', 'identifier_value', 'patient_id', 'verified_at', 'is_deleted']) }}
