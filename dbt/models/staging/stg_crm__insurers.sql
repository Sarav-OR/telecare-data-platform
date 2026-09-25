{%- set payload = ['insurer_name', 'is_active', 'valid_since'] %}
select
    {{ hash_key(['insurer_id']) }}                      as hk_insurer,
    {{ hashdiff(payload) }}                             as hd_insurer,
    insurer_id,
    {{ payload | join(',\n    ') }},
    cast(snapshot_date as timestamp)                    as applied_datetime,
    ingested_at                                         as load_datetime,
    'CRM.insurers'                                      as record_source
from {{ source('silver', 'crm_insurers') }}
