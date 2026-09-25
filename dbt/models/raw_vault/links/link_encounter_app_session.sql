-- Encounter started from an app session (chat / video)
{{ vault_link(source='stg_ehr__encounter', link_hash_key='hk_link_encounter_app',
              foreign_hash_keys=['hk_encounter', 'hk_app_session']) }}
