with source as (
  select * from {{ source('dspace5', 'handle') }}
),

renamed as (
  select
    handle_id,
    handle,
    resource_type_id,
    resource_id,
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
