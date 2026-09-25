{{ vault_sat(source='stg_dev__device_registry', parent_hash_key='hk_device', hashdiff_column='hd_device',
             payload=['device_serial', 'model', 'manufacturer', 'firmware_version', 'partner_id', 'status', 'installed_on']) }}
