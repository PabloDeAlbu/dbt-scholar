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
),
ghost_record as (
  select
    -1 as community_id,
    -1 as logo_bitstream_id,
    false as admin,
    {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
