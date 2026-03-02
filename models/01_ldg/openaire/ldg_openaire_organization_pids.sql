with source as (
  select * from {{ source('openaire', 'organization_pids') }}
),
base as (
  select 
    organization_id::text,
    pid_scheme::text,
    pid_value::text,
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as organization_id,
    '!UNKNOWN'::text as pid_scheme,
    '!UNKNOWN'::text as pid_value,
    {{ dbt_date.today() }} as _load_datetime
)

select * from base
union all
select * from ghost_record
