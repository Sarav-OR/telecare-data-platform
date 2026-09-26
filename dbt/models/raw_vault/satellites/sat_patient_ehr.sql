-- Patient attributes, full history. date_of_birth is PII: masked in Unity Catalog.
{{ config(post_hook="{{ apply_pii_mask('date_of_birth') }}") }}
{{ vault_sat(source='stg_ehr__patient', parent_hash_key='hk_patient', hashdiff_column='hd_patient',
             payload=['patient_id', 'date_of_birth', 'sex', 'canton', 'postal_code', 'preferred_language', 'is_deleted']) }}

             
