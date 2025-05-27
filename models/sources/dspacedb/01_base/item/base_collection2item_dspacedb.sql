with source as (
      select * from {{ source('dspacedb', 'collection2item') }}
),
renamed as (
    select
        {{ adapter.quote("collection_id") }},
        {{ adapter.quote("item_id") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  