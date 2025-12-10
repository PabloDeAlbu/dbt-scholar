with source as (
  select * from {{ source('dspace5', 'metadatavalue') }}
),

renamed as (
  select
    metadata_value_id,
    resource_id,
    metadata_field_id,
    text_value,
    text_lang,
    place,
    authority,
    confidence,
    resource_type_id,
    {{ dbt_date.today() }} as load_datetime
  from source
)

select * from renamed
