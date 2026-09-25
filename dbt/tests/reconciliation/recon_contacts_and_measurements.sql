-- Migration check: no contact or measurement is lost between vault and Kimball.
with checks as (
    select 'contacts' as check_name,
           (select count(*) from {{ ref('bv_call_session') }}) + (select count(*) from {{ ref('bv_app_session') }}) as vault_rows,
           (select count(*) from {{ ref('fact_contact') }}) as kimball_rows
    union all
    select 'measurements',
           (select count(*) from {{ ref('nhl_device_measurement') }}),
           (select count(*) from {{ ref('fact_diagnostic_measurement') }})
)
select * from checks where vault_rows <> kimball_rows
