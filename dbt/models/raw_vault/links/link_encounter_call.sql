-- Encounter started from a phone call (contact -> consultation funnel)
{{ vault_link(source='stg_ehr__encounter', link_hash_key='hk_link_encounter_call',
              foreign_hash_keys=['hk_encounter', 'hk_call']) }}
