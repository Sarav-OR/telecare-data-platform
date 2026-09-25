{#
  Minimal, dependency-free Data Vault 2.0 loading patterns (insert-only).
  Each macro is idempotent: re-running a load never creates duplicates, which
  makes backfills and retries safe.
#}

{# -------------------------------------------------------------------- HUB
   One row per business key, first seen wins. Supports multiple sources
   (e.g. device keys arrive from the device master, assignments and telemetry). #}
{% macro vault_hub(sources, hash_key, business_keys) %}
with unioned as (
    {% for src in sources %}
    select
        {{ hash_key }},
        {{ business_keys | join(', ') }},
        load_datetime,
        record_source
    from {{ ref(src) }}
    where {{ hash_key }} is not null
    {% if is_incremental() %}
      and load_datetime > (select coalesce(max(load_datetime), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),

first_seen as (
    select *
    from unioned
    qualify row_number() over (partition by {{ hash_key }} order by load_datetime, record_source) = 1
)

select f.*
from first_seen f
{% if is_incremental() %}
where not exists (select 1 from {{ this }} t where t.{{ hash_key }} = f.{{ hash_key }})
{% endif %}
{% endmacro %}


{# ------------------------------------------------------------------- LINK #}
{% macro vault_link(source, link_hash_key, foreign_hash_keys) %}
with src as (
    select
        {{ link_hash_key }},
        {{ foreign_hash_keys | join(', ') }},
        load_datetime,
        record_source
    from {{ ref(source) }}
    where {{ link_hash_key }} is not null
    {% if is_incremental() %}
      and load_datetime > (select coalesce(max(load_datetime), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
    qualify row_number() over (partition by {{ link_hash_key }} order by load_datetime) = 1
)

select s.*
from src s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} t where t.{{ link_hash_key }} = s.{{ link_hash_key }})
{% endif %}
{% endmacro %}


{# -------------------------------------------------------------- SATELLITE
   Inserts a row only when the hashdiff changes versus the previous version,
   both inside the current batch and against the latest row already loaded.
   `dependent_key` supports multi-active / effectivity satellites.
   `sequence_column` orders versions: the applied (business) timestamp of a
   snapshot, so several daily extracts loaded in one run keep their true order. #}
{% macro vault_sat(source, parent_hash_key, hashdiff_column, payload, dependent_key=none, sequence_column='applied_datetime') %}
{%- set partition = [parent_hash_key] + ([dependent_key] if dependent_key else []) -%}
with src as (
    select
        {{ partition | join(', ') }},
        {{ hashdiff_column }},
        {{ payload | join(', ') }},
        {{ sequence_column }},
        load_datetime,
        record_source
    from {{ ref(source) }}
    where {{ parent_hash_key }} is not null
    {% if is_incremental() %}
      and load_datetime > (select coalesce(max(load_datetime), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
    qualify row_number() over (partition by {{ partition | join(', ') }}, {{ sequence_column }} order by load_datetime desc, record_source) = 1
),

sequenced as (
    select
        src.*,
        lag({{ hashdiff_column }}) over (partition by {{ partition | join(', ') }} order by {{ sequence_column }}) as prev_hashdiff
    from src
),

changes as (
    select * from sequenced
    where prev_hashdiff is null or prev_hashdiff <> {{ hashdiff_column }}
)

{% if is_incremental() %}
, latest_loaded as (
    select {{ partition | join(', ') }}, {{ hashdiff_column }}
    from {{ this }}
    qualify row_number() over (partition by {{ partition | join(', ') }} order by {{ sequence_column }} desc) = 1
)
{% endif %}

select
    c.{{ partition | join(', c.') }},
    c.{{ hashdiff_column }},
    {% for col in payload %}c.{{ col }},{% endfor %}
    c.{{ sequence_column }},
    c.load_datetime,
    c.record_source
from changes c
{% if is_incremental() %}
left join latest_loaded l
    on {% for k in partition %}l.{{ k }} = c.{{ k }}{% if not loop.last %} and {% endif %}{% endfor %}
where not (c.prev_hashdiff is null and l.{{ hashdiff_column }} = c.{{ hashdiff_column }})
   or l.{{ parent_hash_key }} is null
{% endif %}
{% endmacro %}


{# ------------------------------------------------- NON-HISTORISED LINK
   Immutable events (telephony / app events, device measurements): insert once per
   event key, never updated, no hashdiff needed. #}
{% macro vault_nhl(source, hash_key, foreign_hash_keys, payload) %}
with src as (
    select
        {{ hash_key }},
        {{ foreign_hash_keys | join(', ') }},
        {{ payload | join(', ') }},
        load_datetime,
        record_source
    from {{ ref(source) }}
    where {{ hash_key }} is not null
    {% if is_incremental() %}
      and load_datetime > (select coalesce(max(load_datetime), cast('1900-01-01' as timestamp)) from {{ this }})
    {% endif %}
    qualify row_number() over (partition by {{ hash_key }} order by load_datetime) = 1
)

select s.*
from src s
{% if is_incremental() %}
where not exists (select 1 from {{ this }} t where t.{{ hash_key }} = s.{{ hash_key }})
{% endif %}
{% endmacro %}
