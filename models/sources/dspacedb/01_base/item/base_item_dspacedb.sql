with source as (
      select * from {{ source('dspacedb', 'item') }}
),
renamed as (
    select
        {{ adapter.quote("item_id") }},
        {{ adapter.quote("in_archive") }},
        {{ adapter.quote("withdrawn") }},
        {{ adapter.quote("last_modified") }},
        {{ adapter.quote("discoverable") }},
        {{ adapter.quote("uuid") }},
        {{ adapter.quote("submitter_id") }},
        {{ adapter.quote("owning_collection") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  