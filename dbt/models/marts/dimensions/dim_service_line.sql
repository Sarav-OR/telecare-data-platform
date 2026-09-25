select service_line_code as service_line_key, service_line_name, is_emergency, min_age, max_age
from {{ ref('service_lines') }}
union all select '{{ var("unknown_key") }}', 'Unknown', null, null, null
