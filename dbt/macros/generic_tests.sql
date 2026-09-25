{#
  In-house generic tests (no external packages, so the project builds in
  locked-down networks). Usage examples are in the models' .yml files.
#}

{# Row-level boolean assertion #}
{% test expression_is_true(model, expression, where=none, column_name=none) %}
select *
from {{ model }}
where not ({{ expression }})
{% if where %} and ({{ where }}){% endif %}
{% endtest %}


{# Uniqueness across several columns #}
{% test unique_combination(model, columns) %}
select {{ columns | join(', ') }}, count(*) as n
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}


{# SCD2 integrity: per entity, validity windows must not overlap and exactly
   one row must be current #}
{% test scd2_integrity(model, entity_key, valid_from='valid_from', valid_to='valid_to', current_flag='is_current') %}
with ordered as (
    select
        {{ entity_key }} as entity,
        {{ valid_from }} as vf,
        {{ valid_to }} as vt,
        {{ current_flag }} as cur,
        lead({{ valid_from }}) over (partition by {{ entity_key }} order by {{ valid_from }}) as next_vf
    from {{ model }}
),
gaps_or_overlaps as (
    select entity, 'overlap_or_gap' as issue from ordered
    where next_vf is not null and vt <> next_vf
),
bad_windows as (
    select entity, 'valid_to_before_valid_from' as issue from ordered where vt <= vf
),
current_count as (
    select entity, 'not_exactly_one_current' as issue
    from ordered group by entity
    having sum(case when cur then 1 else 0 end) <> 1
)
select * from gaps_or_overlaps
union all select * from bad_windows
union all select * from current_count
{% endtest %}


{# Reconciliation: row counts of two relations must match (optionally per group,
   optionally within a tolerance in percent) #}
{% test row_count_matches(model, compare_model, group_by=none, compare_group_by=none, where=none, compare_where=none, tolerance_pct=0) %}
{%- set g = group_by or [] -%}
{%- set cg = compare_group_by or g -%}
with a as (
    select {% for c in g %}{{ c }} as g{{ loop.index }}, {% endfor %}count(*) as n
    from {{ model }} {% if where %}where {{ where }}{% endif %}
    {% if g %}group by {% for c in g %}{{ c }}{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}
),
b as (
    select {% for c in cg %}{{ c }} as g{{ loop.index }}, {% endfor %}count(*) as n
    from {{ compare_model }} {% if compare_where %}where {{ compare_where }}{% endif %}
    {% if cg %}group by {% for c in cg %}{{ c }}{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}
)
select
    {% for c in g %}coalesce(a.g{{ loop.index }}, b.g{{ loop.index }}) as g{{ loop.index }}, {% endfor %}
    coalesce(a.n, 0) as model_rows,
    coalesce(b.n, 0) as compare_rows
from a
full outer join b
    on {% if g %}{% for c in g %}a.g{{ loop.index }} = b.g{{ loop.index }}{% if not loop.last %} and {% endif %}{% endfor %}{% else %}1 = 1{% endif %}
where abs(coalesce(a.n, 0) - coalesce(b.n, 0))
      > {{ tolerance_pct }} / 100.0 * greatest(coalesce(a.n, 0), coalesce(b.n, 0))
{% endtest %}


{# Table must not be empty (guards against silent upstream outages) #}
{% test not_empty(model, where=none) %}
select 1 as empty_table
where (select count(*) from {{ model }} {% if where %}where {{ where }}{% endif %}) = 0
{% endtest %}
