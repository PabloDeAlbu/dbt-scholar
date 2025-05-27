with source as (
  select * from {{ source('dspace5', 'metadatavalue') }}
),

renamed as (
  select
    {{ adapter.quote("metadata_value_id") }},
    {{ adapter.quote("resource_id") }},
    {{ adapter.quote("metadata_field_id") }},
    {{ adapter.quote("text_value") }},
    {{ adapter.quote("text_lang") }},
    {{ adapter.quote("place") }},
    {{ adapter.quote("authority") }},
    {{ adapter.quote("confidence") }},
    {{ adapter.quote("resource_type_id") }}
  from source
),

casted as (
  select
    metadata_value_id::varchar,
    resource_id::varchar,
    metadata_field_id::varchar,
    text_value::varchar,
    text_lang::varchar,
    resource_type_id::varchar,
    authority::varchar,
    place::int,
    confidence::int
  from renamed
),

select * from casted
