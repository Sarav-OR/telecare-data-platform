{{ vault_sat(source='stg_ehr__referral', parent_hash_key='hk_referral', hashdiff_column='hd_referral',
             payload=['referral_id', 'encounter_id', 'partner_id', 'referral_type', 'urgency', 'issued_at', 'is_deleted']) }}
