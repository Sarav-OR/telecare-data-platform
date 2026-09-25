{%- set payload = ['partner_type', 'partner_name', 'city', 'postal_code', 'canton', 'connect_enabled',
                   'is_active', 'opened_on', 'deactivated_on'] %}
select
    {{ hash_key(['partner_id']) }}                      as hk_partner,
    {{ hashdiff(payload) }}                             as hd_partner,
    partner_id,
    {{ payload | join(',\n    ') }},
    cast(snapshot_date as timestamp)                    as applied_datetime,
    ingested_at                                         as load_datetime,
    'CRM.partner_api'                                   as record_source
from {{ source('silver', 'crm_partners') }}
