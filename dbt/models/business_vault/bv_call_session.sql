-- Business rule: sessionise telephony events into one row per call.
--   queue time    = QUEUED -> ANSWERED (or -> ABANDONED)
--   service level = answered within var('service_level_seconds') of queuing
--   calls without an END event are closed as TIMEOUT after var('call_timeout_hours')
with ev as (
    select * from {{ ref('nhl_call_event') }}
),

agg as (
    select
        hk_call,
        call_id,
        max(hk_patient)                                                        as hk_caller,
        min(case when event_type = 'OFFERED' then event_ts end)                as offered_at,
        min(case when event_type = 'QUEUED' then event_ts end)                 as queued_at,
        min(case when event_type = 'ANSWERED' then event_ts end)               as answered_at,
        min(case when event_type = 'TRANSFERRED' then event_ts end)            as transferred_at,
        min(case when event_type = 'ABANDONED' then event_ts end)              as abandoned_at,
        min(case when event_type = 'CALLBACK_REQUESTED' then event_ts end)     as callback_requested_at,
        max(case when event_type = 'ENDED' then event_ts end)                  as ended_at,
        max(case when event_type = 'ANSWERED' then agent_login end)            as agent_login,
        max(case when event_type = 'TRANSFERRED' then agent_login end)         as physician_login,
        min(event_ts)                                                          as first_event_at,
        max(event_ts)                                                          as last_event_at,
        max(load_datetime)                                                     as last_loaded_at,
        count(*)                                                               as event_count
    from ev
    group by hk_call, call_id
),

attrs as (   -- attributes of the first event of the call
    select hk_call, queue_code, dialled_line, ivr_language
    from ev
    qualify row_number() over (partition by hk_call order by event_ts, sequence_no) = 1
),

max_seen as (select max(event_ts) as watermark from ev)

select
    a.*,
    t.queue_code,
    t.dialled_line,
    t.ivr_language,
    coalesce(a.offered_at, a.first_event_at)                                   as contact_started_at,
    case
        when a.transferred_at is not null then 'TRANSFERRED'
        when a.answered_at is not null then 'ANSWERED'
        when a.callback_requested_at is not null then 'CALLBACK'
        when a.abandoned_at is not null then 'ABANDONED'
        when a.ended_at is null
             and a.last_event_at < m.watermark - interval '{{ var("call_timeout_hours") }}' hour then 'TIMEOUT'
        else 'IN_PROGRESS'
    end                                                                        as contact_outcome_code,
    {{ seconds_between('a.queued_at', 'coalesce(a.answered_at, a.abandoned_at, a.callback_requested_at)') }} as queue_seconds,
    {{ seconds_between('a.answered_at', 'a.ended_at') }}                       as handle_seconds,
    a.answered_at is not null
        and {{ seconds_between('a.queued_at', 'a.answered_at') }} <= {{ var('service_level_seconds') }} as is_answered_within_sl
from agg a
left join attrs t on t.hk_call = a.hk_call
cross join max_seen m
