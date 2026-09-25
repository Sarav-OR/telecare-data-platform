select channel_code as channel_key, channel_name, is_synchronous, is_digital from {{ ref('channels') }}
union all select '{{ var("unknown_key") }}', 'Unknown / not applicable', null, null
