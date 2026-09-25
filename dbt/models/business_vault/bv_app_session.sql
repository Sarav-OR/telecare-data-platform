-- Business rule: sessionise app events into one row per session; QA/test accounts removed.
with ev as (
    select * from {{ ref('nhl_app_event') }}
    where not coalesce(is_test_account, false)
),

agg as (
    select
        hk_app_session,
        session_id,
        max(hk_patient)                                                                   as hk_app_user,
        max(app_user_id)                                                                  as app_user_id,
        min(server_ts)                                                                    as contact_started_at,
        min(case when event_name = 'booking_created' then server_ts end)                  as booking_created_at,
        min(case when event_name = 'booking_cancelled' then server_ts end)                as booking_cancelled_at,
        min(case when event_name in ('chat_started', 'video_started') then server_ts end) as consultation_started_at,
        max(case when event_name in ('chat_ended', 'video_ended') then server_ts end)     as consultation_ended_at,
        max(case when event_name = 'symptom_check_completed' then 1 else 0 end) = 1       as used_symptom_checker,
        max(platform)                                                                     as platform,
        max(app_version)                                                                  as app_version,
        max(service_line)                                                                 as service_line,
        max(channel)                                                                      as channel,
        max(symptom_category)                                                             as symptom_category,
        max(load_datetime)                                                                as last_loaded_at,
        count(*)                                                                          as event_count
    from ev
    group by hk_app_session, session_id
)

select
    *,
    case
        when consultation_started_at is not null then 'SESSION_COMPLETED'
        when booking_cancelled_at is not null then 'SESSION_CANCELLED'
        else 'SESSION_BROWSE_ONLY'
    end                                                                                   as contact_outcome_code,
    {{ seconds_between('booking_created_at', 'consultation_started_at') }}                as queue_seconds,
    {{ seconds_between('consultation_started_at', 'consultation_ended_at') }}             as handle_seconds
from agg
