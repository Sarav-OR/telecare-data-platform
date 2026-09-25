{#
  Kimball helpers: SCD2 dimensions derived from Data Vault satellites, unknown
  members and point-in-time joins. Satellites already hold one row per real change
  (hashdiff), so a type-2 dimension is simply the satellite with validity windows.
#}

{# SCD2 versions of a satellite: valid_from / valid_to / is_current.
   The first version is open to the past so facts that precede the first extract
   still find a member. #}
{% macro scd2_from_sat(sat_relation, parent_key, sequence_col='applied_datetime') %}
    select
        s.*,
        case when row_number() over (partition by s.{{ parent_key }} order by s.{{ sequence_col }}) = 1
             then cast('1900-01-01 00:00:00' as timestamp)
             else s.{{ sequence_col }} end                                             as valid_from,
        coalesce(lead(s.{{ sequence_col }}) over (partition by s.{{ parent_key }} order by s.{{ sequence_col }}),
                 {{ high_date() }})                                                   as valid_to,
        lead(s.{{ sequence_col }}) over (partition by s.{{ parent_key }} order by s.{{ sequence_col }}) is null
                                                                                      as is_current
    from {{ sat_relation }} s
{% endmacro %}

{# point-in-time join condition: fact timestamp inside the dimension version window #}
{% macro pit(dim_alias, ts) -%}
    {{ ts }} >= {{ dim_alias }}.valid_from and {{ ts }} < {{ dim_alias }}.valid_to
{%- endmacro %}

{# surrogate key of an SCD2 version #}
{% macro scd2_sk(parent_key) -%}
    {{ hash_key([parent_key, 'cast(valid_from as ' ~ dbt.type_string() ~ ')']) }}
{%- endmacro %}
