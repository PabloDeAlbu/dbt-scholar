with source as (
  select * from {{ source('dspace5', 'bitstream') }}
),
renamed as (
  select
    bitstream_id,
    bitstream_format_id,
    size_bytes,
    checksum,
    checksum_algorithm,
    internal_id,
    deleted,
    store_number,
    sequence_id,
    {{ dbt_date.today() }} as load_datetime
  from source
),
ghost_record as (
  select
    -1 as bitstream_id,
    -1 as bitstream_format_id,
    -1 as size_bytes,
    '!UNKNOWN' as checksum,
    '!UNKNOWN' as checksum_algorithm,
    '!UNKNOWN' as internal_id,
    false as deleted,
    -1 as store_number,
    -1 as sequence_id,
    {{ dbt_date.today() }} as load_datetime
)

select * from renamed
union all
select * from ghost_record
