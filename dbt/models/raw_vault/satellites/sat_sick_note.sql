{{ vault_sat(source='stg_ehr__sick_note', parent_hash_key='hk_sick_note', hashdiff_column='hd_sick_note',
             payload=['sick_note_id', 'encounter_id', 'incapacity_pct', 'days', 'valid_from', 'issued_at', 'is_deleted']) }}
