with source as (
  select 
    id,
    scheme,
    value,
    load_datetime
  from {{ source('openaire', 'researchproduct_pids')}}
),

casted as (
  select
    id::text as researchproduct_id,
    scheme::text as scheme,
    value::text as value,
    {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
  from source
)

select * from casted
