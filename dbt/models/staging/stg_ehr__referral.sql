{%- set payload = ['referral_type', 'urgency', 'issued_at', 'is_deleted'] %}
select
    {{ hash_key(['referral_id']) }}                     as hk_referral,
    {{ hash_key(['encounter_id']) }}                    as hk_encounter,
    {{ hash_key(['partner_id']) }}                      as hk_partner,
    {{ hash_key(['referral_id', 'encounter_id', 'partner_id']) }} as hk_link_referral,
    {{ hashdiff(payload) }}                             as hd_referral,
    referral_id,
    encounter_id,
    partner_id,
    {{ payload | join(',\n    ') }},
    modified_at                                         as applied_datetime,
    ingested_at                                         as load_datetime,
    'EHR.referral'                                      as record_source
from {{ source('silver', 'ehr_referral') }}
