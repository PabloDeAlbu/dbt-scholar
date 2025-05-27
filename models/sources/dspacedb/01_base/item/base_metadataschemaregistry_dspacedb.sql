with source as (
      select * from {{ source('dspacedb', 'metadataschemaregistry') }}
),
renamed as (
    select
        {{ adapter.quote("metadata_schema_id") }},
        {{ adapter.quote("namespace") }},
        {{ adapter.quote("short_id") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  