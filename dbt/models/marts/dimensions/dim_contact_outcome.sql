select contact_outcome_code as contact_outcome_key, contact_outcome_name, is_answered, is_lost
from {{ ref('contact_outcomes') }}
union all select '{{ var("unknown_key") }}', 'Unknown', null, null
