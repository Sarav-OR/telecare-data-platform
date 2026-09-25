select cast(triage_level as {{ dbt.type_string() }}) as triage_level_key, triage_level, triage_name, target_response, is_urgent
from {{ ref('triage_levels') }}
union all select '{{ var("unknown_key") }}', null, 'Not triaged / unknown', null, null
