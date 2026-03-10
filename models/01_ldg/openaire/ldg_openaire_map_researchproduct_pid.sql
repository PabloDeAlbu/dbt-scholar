with source as (
  select 
    *
  from {{ source('openaire', 'map_researchproduct_pid')}}
),

casted as (
  select
    id::text as researchproduct_id,
    scheme::text as scheme,
    value::text as value,
    _load_datetime::timestamp
  from source
),
ghost_record as (
  select
    '!UNKNOWN'::text as researchproduct_id,
    '!UNKNOWN'::text as scheme,
    '!UNKNOWN'::text as value,
    {{ dbt_date.today() }} as _load_datetime
)

select * from casted
union all
select * from ghost_record
