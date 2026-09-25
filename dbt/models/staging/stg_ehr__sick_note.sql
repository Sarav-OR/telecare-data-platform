{%- set payload = ['incapacity_pct', 'days', 'valid_from', 'issued_at', 'is_deleted'] %}
select
    {{ hash_key(['sick_note_id']) }}                    as hk_sick_note,
    {{ hash_key(['encounter_id']) }}                    as hk_encounter,
    {{ hash_key(['sick_note_id', 'encounter_id']) }}    as hk_link_sick_note,
    {{ hashdiff(payload) }}                             as hd_sick_note,
    sick_note_id,
    encounter_id,
    {{ payload | join(',\n    ') }},
    modified_at                                         as applied_datetime,
    ingested_at                                         as load_datetime,
    'EHR.sick_note'                                     as record_source
from {{ source('silver', 'ehr_sick_note') }}
