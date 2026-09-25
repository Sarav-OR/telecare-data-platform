-- Hub: every patient identity. Business-key collision code (patient_bkcc) keeps the
-- three key spaces apart: EHR patient ids, telephony phone hashes (TEL), app user ids (APP).
-- sal_patient (same-as link) resolves TEL/APP identities to the EHR master patient.
{{ vault_hub(sources=['stg_ehr__patient', 'stg_ehr__patient_identifier', 'stg_tel__call_events', 'stg_app__app_events'],
             hash_key='hk_patient', business_keys=['patient_bkcc', 'patient_bk']) }}
