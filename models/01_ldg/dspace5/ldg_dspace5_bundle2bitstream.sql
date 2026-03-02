with source as (
  select * from {{ source('dspace5', 'bundle2bitstream') }}
),
renamed as (
  select
    id,
    bundle_id,
    bitstream_id,
    bitstream_order,
    {{ dbt_date.today() }} as _load_datetime
  from source
),
ghost_record as (
  select
    -1 as id,
    -1 as bundle_id,
    -1 as bitstream_id,
    -1 as bitstream_order,
    {{ dbt_date.today() }} as _load_datetime
)

select * from renamed
union all
select * from ghost_record
