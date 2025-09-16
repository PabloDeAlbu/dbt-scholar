with base as (
  select 
    organization_id::text,
    acronym::text,
    {{ adapter.quote("legalName") }}::text as legalname,
    load_datetime::timestamp
  from {{ source('openaire', 'organization') }}
)

select * from base
