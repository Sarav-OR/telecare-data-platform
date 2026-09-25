{#
  Environment-aware schema naming.
    prod / ci targets : <custom_schema>                    e.g. marts
    everything else   : <target.schema>_<custom_schema>    e.g. dbt_sarav_marts
  Keeps developers isolated in the dev catalog while production uses clean names.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name in ('prod', 'ci') -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
