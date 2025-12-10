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
)

select * from renamed
