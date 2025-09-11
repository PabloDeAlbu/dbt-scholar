with source as (
  select * from {{ source('openaire', 'researchproduct_instances') }}
),

filtered as (
  select 
    id,
    urls as url,
    load_datetime
  from source
  where urls is not null
),

renamed as (
  select 
    id as researchproduct_id,
    url,
    load_datetime
  from filtered
),

casted as (
  select 
  researchproduct_id::text,
  url::text,
  {{ dbt_date.convert_timezone("load_datetime") }} as load_datetime
  from renamed
)

select * from casted