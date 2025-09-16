with base as (
  select 
    organization_id::text,
    pid_scheme::text,
    pid_value::text,
    load_datetime::timestamp
  from {{ source('openaire', 'organization_pids') }}
)

select * from base
