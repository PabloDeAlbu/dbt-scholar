with source as (
  select * from {{ source('dspace5', 'item') }}
),
renamed as (
  select
    {{ adapter.quote("item_id") }},
    {{ adapter.quote("submitter_id") }},
    {{ adapter.quote("in_archive") }},
    {{ adapter.quote("withdrawn") }},
    {{ adapter.quote("last_modified") }},
    {{ adapter.quote("owning_collection") }},
    {{ adapter.quote("discoverable") }}
  from source
),

casted as (
  select
    item_id::varchar,
    submitter_id::varchar,
    withdrawn::varchar,
    owning_collection::varchar,
    in_archive::boolean,
    discoverable::boolean,
    {{ dbt_date.convert_timezone("last_modified") }} as last_modified,
    {{ dbt_date.now() }} as load_datetime
  from renamed
)

select * from casted
