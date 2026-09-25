{{ config(liquid_clustered_by=['server_ts']) }}
{{ vault_nhl(source='stg_app__app_events', hash_key='hk_app_event', foreign_hash_keys=['hk_app_session', 'hk_patient'],
             payload=['event_id', 'session_id', 'app_user_id', 'event_name', 'client_ts', 'server_ts', 'platform',
                      'app_version', 'service_line', 'channel', 'symptom_category', 'triage_score',
                      'wait_estimate_min', 'is_test_account']) }}
