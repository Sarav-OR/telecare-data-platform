{{ config(liquid_clustered_by=['measured_at']) }}
{{ vault_nhl(source='stg_dev__measurements', hash_key='hk_measurement',
             foreign_hash_keys=['hk_device', 'hk_encounter', 'hk_partner'],
             payload=['measurement_id', 'device_serial', 'partner_id', 'encounter_ref', 'metric_code', 'metric_value',
                      'unit', 'original_value', 'original_unit', 'measured_at', 'received_at', 'firmware_version',
                      'is_late_arriving']) }}
