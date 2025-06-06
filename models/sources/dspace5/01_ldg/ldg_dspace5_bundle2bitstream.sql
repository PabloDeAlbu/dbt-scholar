with source as (
  select * from {{ source('dspace5', 'bundle2bitstream') }}
),

renamed as (
  select
    id,
    bundle_id,
    bitstream_id,
    bitstream_order,
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
