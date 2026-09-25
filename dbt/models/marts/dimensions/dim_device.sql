with versions as (
    {{ scd2_from_sat(ref('sat_device'), 'hk_device') }}
)
select
    {{ scd2_sk('hk_device') }}                                     as device_sk,
    hk_device, device_serial, model, manufacturer, firmware_version, partner_id, status, installed_on,
    valid_from, valid_to, is_current
from versions
union all
select '{{ var("unknown_key") }}', null, 'UNKNOWN', 'Unknown', null, null, null, null, null,
       cast('1900-01-01 00:00:00' as timestamp), {{ high_date() }}, true
