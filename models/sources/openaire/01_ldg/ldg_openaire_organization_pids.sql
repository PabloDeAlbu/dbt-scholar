with base as (
  select 
    *from {{ source('openaire', 'organization_pids') }}
)

select * from base
