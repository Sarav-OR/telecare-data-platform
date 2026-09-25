{%- set payload = ['model', 'manufacturer', 'firmware_version', 'partner_id', 'status', 'installed_on'] %}
select
    {{ hash_key(['device_serial']) }}                   as hk_device,
    {{ hashdiff(payload) }}                             as hd_device,
    device_serial,
    {{ payload | join(',\n    ') }},
    cast(snapshot_date as timestamp)                    as applied_datetime,
    ingested_at                                         as load_datetime,
    'DEV.device_registry'                               as record_source
from {{ source('silver', 'device_registry') }}
