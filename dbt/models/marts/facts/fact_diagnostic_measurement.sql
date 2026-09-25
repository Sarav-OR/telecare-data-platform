{#
  fact_diagnostic_measurement - one row per pharmacy device measurement.
  Alert level uses the clinical thresholds in dim_metric (business-owned seed), not
  the plausibility ranges of the ingestion DQ rules (those reject sensor faults).
#}
{{ config(unique_key='measurement_key', liquid_clustered_by=['date_key']) }}

with m as (
    select *
    from {{ ref('nhl_device_measurement') }}
    {% if is_incremental() %}where {{ lookback_filter('load_datetime') }}{% endif %}
),

encounter_patient as (
    select h.hk_encounter, h.encounter_id, le.hk_patient
    from {{ ref('hub_encounter') }} h
    join {{ ref('link_encounter') }} le on le.hk_encounter = h.hk_encounter
    qualify row_number() over (partition by h.hk_encounter order by le.load_datetime desc) = 1
),

base as (
    select m.*, ep.encounter_id, ep.hk_patient, {{ to_local_ts('m.measured_at') }} as measured_at_local
    from m
    left join encounter_patient ep on ep.hk_encounter = m.hk_encounter
)

select
    b.measurement_id                                         as measurement_key,
    {{ date_key('b.measured_at_local') }}                    as date_key,
    {{ time_key('b.measured_at_local') }}                    as time_key,
    coalesce(d.device_sk, '{{ var("unknown_key") }}')        as device_sk,
    coalesce(pa.partner_sk, '{{ var("unknown_key") }}')      as partner_sk,
    coalesce(p.patient_sk, '{{ var("unknown_key") }}')       as patient_sk,
    coalesce(mt.metric_key, '{{ var("unknown_key") }}')      as metric_key,
    'PHARMACY_CONNECT'                                       as service_line_key,
    b.encounter_id                                           as encounter_key,
    b.measured_at                                            as measured_at_utc,
    b.measured_at_local,
    b.metric_value,
    b.unit,
    b.original_value,
    b.original_unit,
    case
        when b.metric_value < mt.critical_low or b.metric_value > mt.critical_high then 'CRITICAL'
        when b.metric_value < mt.warning_low or b.metric_value > mt.warning_high then 'WARNING'
        else 'NORMAL'
    end                                                      as alert_level,
    {{ seconds_between('b.measured_at', 'b.received_at') }}  as latency_seconds,
    b.is_late_arriving,
    b.load_datetime                                          as _loaded_at
from base b
left join {{ ref('dim_device') }} d   on d.hk_device = b.hk_device and {{ pit('d', 'b.measured_at') }}
left join {{ ref('dim_partner') }} pa on pa.hk_partner = b.hk_partner and {{ pit('pa', 'b.measured_at') }}
left join {{ ref('dim_patient') }} p  on p.hk_patient = b.hk_patient and {{ pit('p', 'b.measured_at') }}
left join {{ ref('dim_metric') }} mt  on mt.metric_key = b.metric_code
