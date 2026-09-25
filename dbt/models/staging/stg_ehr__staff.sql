{%- set payload = ['staff_login', 'role', 'specialty', 'team', 'languages', 'employment_pct',
                   'hired_at', 'left_at', 'is_deleted'] %}
select
    {{ hash_key(['staff_id']) }}                        as hk_staff,
    {{ hashdiff(payload) }}                             as hd_staff,
    staff_id,
    {{ payload | join(',\n    ') }},
    modified_at                                         as applied_datetime,
    ingested_at                                         as load_datetime,
    'EHR.staff'                                         as record_source
from {{ source('silver', 'ehr_staff') }}
