select icd10_code as diagnosis_key, icd10_code, description, block_code, block_name, chapter_code, chapter_name,
       symptom_category
from {{ ref('icd10_codes') }}
union all select '{{ var("unknown_key") }}', null, 'Unknown / invalid ICD-10 code', null, null, null, 'Unknown', 'UNKNOWN'
