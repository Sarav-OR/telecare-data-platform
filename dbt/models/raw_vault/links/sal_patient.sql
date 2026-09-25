-- Same-as link: a telephony (TEL) or app (APP) identity is the same person as an EHR
-- patient. One phone can map to several patients (a parent calling for children), so the
-- relationship is many-to-many; the business vault decides per contact.
{{ vault_link(source='stg_ehr__patient_identifier', link_hash_key='hk_sal_patient',
              foreign_hash_keys=['hk_patient_master', 'hk_patient']) }}
