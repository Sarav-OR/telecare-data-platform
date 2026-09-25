-- Encounter <-> patient <-> treating clinician
{{ vault_link(source='stg_ehr__encounter', link_hash_key='hk_link_encounter',
              foreign_hash_keys=['hk_encounter', 'hk_patient', 'hk_staff']) }}
