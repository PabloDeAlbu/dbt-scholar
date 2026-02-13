with source as (
  select * from {{ source('openaire', 'researchproduct_originalid') }}
),
base as (
  select 
    id::text as researchproduct_id,
    {{ adapter.quote("originalIds") }}::text as original_id,
    load_datetime::timestamp
 from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as original_id,
    {{ dbt_date.today() }} as load_datetime
)

select * from base
union all
select * from ghost_record
