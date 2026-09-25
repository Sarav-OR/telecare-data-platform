with current_rx as (
    select s.*
    from {{ ref('sat_prescription') }} s
    qualify row_number() over (partition by s.hk_prescription order by s.applied_datetime desc) = 1
)
select
    rx.encounter_id                                                  as encounter_key,
    rx.prescription_id,
    coalesce(m.medication_key, '{{ var("unknown_key") }}')           as medication_key,
    rx.quantity,
    rx.pharmacy_partner_id,
    rx.issued_at                                                     as issued_at_utc,
    1.0 / count(*) over (partition by rx.encounter_id)               as weighting_factor
from current_rx rx
left join {{ ref('dim_medication') }} m on m.medication_key = rx.atc_code
where not rx.is_deleted
