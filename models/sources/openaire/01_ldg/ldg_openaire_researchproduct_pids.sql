with source as (
  select 
    *
  from {{ source('openaire', 'researchproduct_pids')}}
),

casted as (
  select
    id::text as researchproduct_id,
    scheme::text as scheme,
    value::text as value,
    load_datetime::timestamp
  from source
)

select * from casted
