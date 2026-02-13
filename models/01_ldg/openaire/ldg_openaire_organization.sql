with source as (
  select * from {{ source('openaire', 'organization') }}
),
base as (
  select 
    organization_id::text,
    acronym::text,
    {{ adapter.quote("legalName") }}::text as legalname,
    load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as organization_id,
    '!UNKNOWN'::text as acronym,
    '!UNKNOWN'::text as legalname,
    {{ dbt_date.today() }} as load_datetime
)

select * from base
union all
select * from ghost_record
