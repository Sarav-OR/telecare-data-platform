{{ vault_sat(source='stg_ehr__prescription', parent_hash_key='hk_prescription', hashdiff_column='hd_prescription',
             payload=['prescription_id', 'encounter_id', 'atc_code', 'quantity', 'pharmacy_partner_id', 'issued_at', 'is_deleted']) }}
