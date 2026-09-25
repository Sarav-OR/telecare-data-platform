select queue_code as queue_key, queue_name, service_line_code, language, skill from {{ ref('queues') }}
union all select '{{ var("unknown_key") }}', 'Unknown / not applicable (app)', null, null, null
