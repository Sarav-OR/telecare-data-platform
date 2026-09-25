{#
  Small cross-engine helpers so the same SQL runs on Databricks (dbt Cloud) and
  DuckDB (CI). Everything else is ANSI SQL that both engines share (QUALIFY, ||,
  CASE, window functions).
#}

{# UTC timestamp -> business-facing local Swiss time #}
{% macro to_local_ts(col) -%}
    {{ adapter.dispatch('to_local_ts', 'telecare')(col) }}
{%- endmacro %}
{% macro default__to_local_ts(col) -%}
    from_utc_timestamp({{ col }}, '{{ var("local_timezone") }}')
{%- endmacro %}
{% macro duckdb__to_local_ts(col) -%}
    (({{ col }} AT TIME ZONE 'UTC') AT TIME ZONE '{{ var("local_timezone") }}')
{%- endmacro %}

{# yyyymmdd integer key of a date / timestamp #}
{% macro date_key(col) -%}
    cast(extract(year from {{ col }}) * 10000 + extract(month from {{ col }}) * 100 + extract(day from {{ col }}) as int)
{%- endmacro %}

{# hhmm integer key of a timestamp #}
{% macro time_key(col) -%}
    cast(extract(hour from {{ col }}) * 100 + extract(minute from {{ col }}) as int)
{%- endmacro %}

{# exact age in years at a given date #}
{% macro age_at(dob, at_date) -%}
    (extract(year from {{ at_date }}) - extract(year from {{ dob }})
     - case when extract(month from {{ at_date }}) * 100 + extract(day from {{ at_date }})
                 < extract(month from {{ dob }}) * 100 + extract(day from {{ dob }}) then 1 else 0 end)
{%- endmacro %}

{% macro age_band(age) -%}
    case when {{ age }} is null then 'Unknown'
         when {{ age }} <= 5  then '00-05'
         when {{ age }} <= 15 then '06-15'
         when {{ age }} <= 30 then '16-30'
         when {{ age }} <= 45 then '31-45'
         when {{ age }} <= 65 then '46-65'
         when {{ age }} <= 80 then '66-80'
         else '81+' end
{%- endmacro %}

{% macro seconds_between(start_col, end_col) -%}
    {{ adapter.dispatch('seconds_between', 'telecare')(start_col, end_col) }}
{%- endmacro %}
{% macro default__seconds_between(start_col, end_col) -%}
    timestampdiff(SECOND, {{ start_col }}, {{ end_col }})
{%- endmacro %}
{% macro duckdb__seconds_between(start_col, end_col) -%}
    datediff('second', {{ start_col }}, {{ end_col }})
{%- endmacro %}

{% macro high_date() -%}
    cast('{{ var("high_date") }}' as timestamp)
{%- endmacro %}

{# incremental fact filter: rows loaded within the lookback window of the last run #}
{% macro lookback_filter(load_col, target_col='_loaded_at') -%}
    {{ load_col }} > (
        select coalesce(max({{ target_col }}), cast('1900-01-01' as timestamp))
               - interval '{{ var("lookback_hours") }}' hour
        from {{ this }})
{%- endmacro %}

{# Unity Catalog column mask for PII columns (no-op outside Databricks) #}
{% macro apply_pii_mask(column) -%}
    {% if target.type == 'databricks' %}
    alter table {{ this }} alter column {{ column }} set mask {{ target.database }}.ops.mask_date_of_birth
    {% endif %}
{%- endmacro %}

{# ISO day of week: 1 = Monday ... 7 = Sunday #}
{% macro iso_dow(col) -%}
    {{ adapter.dispatch('iso_dow', 'telecare')(col) }}
{%- endmacro %}
{% macro default__iso_dow(col) -%} extract(DAYOFWEEK_ISO from {{ col }}) {%- endmacro %}
{% macro duckdb__iso_dow(col) -%} isodow({{ col }}) {%- endmacro %}
