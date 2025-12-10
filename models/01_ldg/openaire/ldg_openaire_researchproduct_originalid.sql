with base as (
  select 
    id::text as researchproduct_id,
    {{ adapter.quote("originalIds") }}::text as original_id,
    load_datetime::timestamp
 from {{ source('openaire', 'researchproduct_originalid') }}
)

select * from base
