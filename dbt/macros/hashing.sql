{#
  Data Vault hashing.

  * business keys are trimmed, upper-cased and NULL-safe before hashing, so the
    same key from two sources always produces the same hash key
  * SHA-256 on every platform (Databricks sha2, DuckDB sha256) -> the CI build
    on DuckDB produces byte-identical keys to production
  * the delimiter prevents collisions such as ('AB','C') vs ('A','BC')
#}

{%- macro _hash_input(columns, upper=true) -%}
    {%- set parts = [] -%}
    {%- for col in columns -%}
        {%- set expr = "coalesce(nullif(trim(cast(" ~ col ~ " as " ~ dbt.type_string() ~ ")), ''), '^^')" -%}
        {%- if upper -%}{%- set expr = "upper(" ~ expr ~ ")" -%}{%- endif -%}
        {%- do parts.append(expr) -%}
    {%- endfor -%}
    {{ parts | join(" || '||' || ") }}
{%- endmacro -%}

{%- macro hash_key(columns) -%}
    {{ adapter.dispatch('sha256', 'telecare')(_hash_input(columns, upper=true)) }}
{%- endmacro -%}

{# hashdiff keeps case: a changed spelling of an attribute is a real change #}
{%- macro hashdiff(columns) -%}
    {{ adapter.dispatch('sha256', 'telecare')(_hash_input(columns | sort, upper=false)) }}
{%- endmacro -%}

{%- macro default__sha256(expr) -%} sha2({{ expr }}, 256) {%- endmacro -%}
{%- macro databricks__sha256(expr) -%} sha2({{ expr }}, 256) {%- endmacro -%}
{%- macro duckdb__sha256(expr) -%} sha256({{ expr }}) {%- endmacro -%}
