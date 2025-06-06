with source as (
  select * from {{ source('dspace5', 'community') }}
),

renamed as (
  select
    community_id,
    logo_bitstream_id,
    admin,
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
