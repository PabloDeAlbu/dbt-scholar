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
    
    {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
  from source
),

renamed as (
  select
    {{ adapter.quote("researchproduct_id") }} as researchproduct_id,
    {{ adapter.quote("scheme") }} as scheme,
    {{ adapter.quote("value") }} as value,
    load_datetime
  from casted
)

select * from renamed
