with source as (
  select * from {{ source('dspace5', 'metadataschemaregistry') }}
),

renamed as (
select
  {{ adapter.quote("metadata_schema_id") }},
  {{ adapter.quote("namespace") }},
  {{ adapter.quote("short_id") }}
  from source
),

casted as (
  select
    metadata_schema_id::varchar,
    namespace::varchar,
    short_id::varchar
  from renamed
)

select * from casted
