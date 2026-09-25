{%- set payload = ['atc_code', 'quantity', 'pharmacy_partner_id', 'issued_at', 'is_deleted'] %}
select
    {{ hash_key(['prescription_id']) }}                 as hk_prescription,
    {{ hash_key(['encounter_id']) }}                    as hk_encounter,
    {{ hash_key(['pharmacy_partner_id']) }}             as hk_partner,
    {{ hash_key(['prescription_id', 'encounter_id', 'pharmacy_partner_id']) }} as hk_link_prescription,
    {{ hashdiff(payload) }}                             as hd_prescription,
    prescription_id,
    encounter_id,
    {{ payload | join(',\n    ') }},
    modified_at                                         as applied_datetime,
    ingested_at                                         as load_datetime,
    'EHR.prescription'                                  as record_source
from {{ source('silver', 'ehr_prescription') }}
