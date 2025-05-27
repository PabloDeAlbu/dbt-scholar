with source as (
      select * from {{ source('dspacedb', 'metadatavalue') }}
),
renamed as (
    select
        {{ adapter.quote("metadata_value_id") }},
        {{ adapter.quote("metadata_field_id") }},
        {{ adapter.quote("text_value") }},
        {{ adapter.quote("text_lang") }},
        {{ adapter.quote("place") }},
        {{ adapter.quote("authority") }},
        {{ adapter.quote("confidence") }},
        {{ adapter.quote("dspace_object_id") }},
        {{ adapter.quote("load_datetime") }}

    from source
)
select * from renamed
  