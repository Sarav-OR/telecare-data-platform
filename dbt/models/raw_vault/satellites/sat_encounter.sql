-- Every state of an encounter: opened, closed, re-coded, soft-deleted
{{ vault_sat(source='stg_ehr__encounter', parent_hash_key='hk_encounter', hashdiff_column='hd_encounter',
             payload=['encounter_id', 'patient_id', 'staff_id', 'channel', 'service_line', 'contact_system',
                      'contact_ref', 'started_at', 'ended_at', 'triage_level', 'disposition_code', 'plan_code',
                      'tariff_amount_chf', 'status', 'is_deleted']) }}
