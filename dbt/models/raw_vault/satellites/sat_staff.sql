{{ vault_sat(source='stg_ehr__staff', parent_hash_key='hk_staff', hashdiff_column='hd_staff',
             payload=['staff_id', 'staff_login', 'role', 'specialty', 'team', 'languages', 'employment_pct',
                      'hired_at', 'left_at', 'is_deleted']) }}
