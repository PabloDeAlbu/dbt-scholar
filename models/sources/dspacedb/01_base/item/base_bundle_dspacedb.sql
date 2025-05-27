with source as (
      select * from {{ source('dspacedb', 'bundle') }}
),
renamed as (
    select
        {{ adapter.quote("bundle_id") }},
        {{ adapter.quote("uuid") }},
        {{ adapter.quote("primary_bitstream_id") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  