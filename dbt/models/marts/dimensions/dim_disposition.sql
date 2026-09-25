select disposition_code as disposition_key, disposition_name, care_setting, is_resolved_remotely, sort_order
from {{ ref('dispositions') }}
union all select '{{ var("unknown_key") }}', 'No disposition (cancelled / open)', null, false, 99
