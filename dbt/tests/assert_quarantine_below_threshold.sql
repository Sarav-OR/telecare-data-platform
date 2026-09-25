-- Data quality monitor: share of quarantined records per feed stays below 1 %.
{{ config(severity='warn') }}
select dataset, sum(failed_rows) as failed, max(total_rows) as total
from {{ source('ops', 'dq_rule_results') }}
where severity = 'error'
group by dataset
having sum(failed_rows) > 0.01 * sum(total_rows)
