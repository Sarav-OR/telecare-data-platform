select metric_code as metric_key, metric_name, standard_unit, critical_low, warning_low, warning_high, critical_high
from {{ ref('metric_reference') }}
union all select '{{ var("unknown_key") }}', 'Unknown', null, null, null, null, null
