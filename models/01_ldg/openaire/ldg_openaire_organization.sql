with source as (
  select * from {{ source('openaire', 'organization') }}
),
base as (
  select 
    organization_id::text,
    acronym::text,
    {{ adapter.quote("legalName") }}::text as legalname,
    dv_load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as organization_id,
    '!UNKNOWN'::text as acronym,
    '!UNKNOWN'::text as legalname,
    {{ dbt_date.today() }} as dv_load_datetime
)

select * from base
union all
select * from ghost_record
