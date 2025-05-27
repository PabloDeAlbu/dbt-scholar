with source as (
      select * from {{ source('dspacedb', 'metadatafieldregistry') }}
),
renamed as (
    select
        {{ adapter.quote("metadata_field_id") }},
        {{ adapter.quote("metadata_schema_id") }},
        {{ adapter.quote("element") }},
        {{ adapter.quote("qualifier") }},
        {{ adapter.quote("scope_note") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  