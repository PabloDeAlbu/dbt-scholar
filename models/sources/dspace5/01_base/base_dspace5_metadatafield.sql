with source as (
  select * from {{ source('dspace5', 'metadatafieldregistry') }}
),

renamed as (
  select
    {{ adapter.quote("metadata_field_id") }},
    {{ adapter.quote("metadata_schema_id") }},
    {{ adapter.quote("element") }},
    {{ adapter.quote("qualifier") }},
    {{ adapter.quote("scope_note") }}

  from source
),

casted as (
  select
    metadata_field_id::varchar,
    metadata_schema_id::varchar,
    element::varchar,
    qualifier::varchar,
    scope_note::varchar
  from renamed
)

select * from casted
