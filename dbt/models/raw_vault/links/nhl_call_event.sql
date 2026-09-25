-- Non-historised link: immutable telephony events
{{ config(liquid_clustered_by=['event_ts']) }}
{{ vault_nhl(source='stg_tel__call_events', hash_key='hk_call_event', foreign_hash_keys=['hk_call', 'hk_patient'],
             payload=['event_id', 'call_id', 'sequence_no', 'event_type', 'event_ts', 'dialled_line', 'queue_code',
                      'agent_login', 'ivr_language']) }}
