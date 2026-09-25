{{ config(liquid_clustered_by=['date_key']) }}
with days as (
    {{ dbt.date_spine('day', "cast('2025-01-01' as date)", "cast('2028-01-01' as date)") }}
)
select
    {{ date_key('d.date_day') }}                                   as date_key,
    cast(d.date_day as date)                                       as calendar_date,
    extract(year from d.date_day)                                  as year,
    extract(quarter from d.date_day)                               as quarter,
    extract(month from d.date_day)                                 as month,
    extract(day from d.date_day)                                   as day_of_month,
    {{ iso_dow('d.date_day') }}                                as iso_day_of_week,   -- 1 = Monday
    case {{ iso_dow('d.date_day') }} when 1 then 'Monday' when 2 then 'Tuesday' when 3 then 'Wednesday'
         when 4 then 'Thursday' when 5 then 'Friday' when 6 then 'Saturday' else 'Sunday' end as day_name,
    extract(week from d.date_day)                                  as iso_week,
    {{ iso_dow('d.date_day') }} >= 6                           as is_weekend,
    h.holiday_date is not null                                     as is_public_holiday,
    h.holiday_name,
    {{ iso_dow('d.date_day') }} >= 6 or h.holiday_date is not null as is_weekend_or_holiday
from days d
left join {{ ref('swiss_public_holidays') }} h
    on h.holiday_date = cast(d.date_day as date) and h.scope = 'national'
