{{ vault_sat(source='stg_crm__partners', parent_hash_key='hk_partner', hashdiff_column='hd_partner',
             payload=['partner_id', 'partner_type', 'partner_name', 'city', 'postal_code', 'canton', 'connect_enabled',
                      'is_active', 'opened_on', 'deactivated_on']) }}
