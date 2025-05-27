with source as (
      select * from {{ source('dspacedb', 'bundle2bitstream') }}
),
renamed as (
    select
        {{ adapter.quote("bitstream_order_legacy") }},
        {{ adapter.quote("bundle_id") }},
        {{ adapter.quote("bitstream_id") }},
        {{ adapter.quote("bitstream_order") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  