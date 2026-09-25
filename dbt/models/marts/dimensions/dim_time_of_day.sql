-- One row per minute of the (local Swiss) day; night tariff 19:00-07:00
with minutes as (
    {{ dbt.date_spine('minute', "cast('2026-01-01 00:00:00' as timestamp)", "cast('2026-01-02 00:00:00' as timestamp)") }}
)
select
    {{ time_key('m.date_minute') }}                                as time_key,
    extract(hour from m.date_minute)                               as hour_of_day,
    extract(minute from m.date_minute)                             as minute_of_hour,
    case when extract(hour from m.date_minute) < 7  then 'Night'
         when extract(hour from m.date_minute) < 12 then 'Morning'
         when extract(hour from m.date_minute) < 17 then 'Afternoon'
         when extract(hour from m.date_minute) < 19 then 'Early evening'
         else 'Evening' end                                        as daypart,
    extract(hour from m.date_minute) < 7 or extract(hour from m.date_minute) >= 19 as is_night_tariff
from minutes m
