with source as (
      select * from {{ source('dspacedb', 'bitstream') }}
),
renamed as (
    select
        {{ adapter.quote("bitstream_id") }},
        {{ adapter.quote("bitstream_format_id") }},
        {{ adapter.quote("checksum") }},
        {{ adapter.quote("checksum_algorithm") }},
        {{ adapter.quote("internal_id") }},
        {{ adapter.quote("deleted") }},
        {{ adapter.quote("store_number") }},
        {{ adapter.quote("sequence_id") }},
        {{ adapter.quote("size_bytes") }},
        {{ adapter.quote("uuid") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  