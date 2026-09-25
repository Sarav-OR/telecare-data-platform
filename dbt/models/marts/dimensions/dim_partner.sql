with versions as (
    {{ scd2_from_sat(ref('sat_partner'), 'hk_partner') }}
)
select
    {{ scd2_sk('hk_partner') }}                                    as partner_sk,
    hk_partner, partner_id, partner_type, partner_name, city, postal_code, canton as canton_code,
    connect_enabled, is_active, opened_on, deactivated_on, valid_from, valid_to, is_current
from versions
union all
select '{{ var("unknown_key") }}', null, 'UNKNOWN', 'Unknown', 'Unknown', null, null, null, null, null, null, null,
       cast('1900-01-01 00:00:00' as timestamp), {{ high_date() }}, true
