-- Bridge weighting factors must sum to 1 per encounter (no double counting).
select encounter_key, sum(weighting_factor) as total_weight
from {{ ref('bridge_encounter_diagnosis') }}
group by encounter_key
having abs(sum(weighting_factor) - 1.0) > 0.0001
