select atc_code as medication_key, atc_code, substance, level1_code, level1_name, level2_code, level2_name,
       is_antibiotic
from {{ ref('atc_codes') }}
union all select '{{ var("unknown_key") }}', null, 'Unknown', null, 'Unknown', null, 'Unknown', null
