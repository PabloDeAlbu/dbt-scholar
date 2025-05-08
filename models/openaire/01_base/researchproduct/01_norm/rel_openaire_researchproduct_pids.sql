with source as (
  select 
    id,
    scheme,
    value,
    load_datetime
  from {{ source('openaire', 'rel_researchproduct_pids')}}
),

casted as (
  select
    id::varchar as researchproduct_id,
    scheme::varchar as scheme,
    value::varchar as value,
    load_datetime::timestamp as load_datetime
  from source
),

renamed as (
  select
    COALESCE({{ adapter.quote("researchproduct_id") }}, 'NO DATA') as researchproduct_id,
    COALESCE({{ adapter.quote("scheme") }}, 'NO DATA') as scheme,
    COALESCE({{ adapter.quote("value") }}, 'NO DATA') as value,
    load_datetime
  from casted
)

select * from renamed
