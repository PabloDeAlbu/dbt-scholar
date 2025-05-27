with source as (
      select * from {{ source('dspacedb', 'item2bundle') }}
),
renamed as (
    select
        {{ adapter.quote("bundle_id") }},
        {{ adapter.quote("item_id") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  