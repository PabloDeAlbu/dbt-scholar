WITH base AS (
    SELECT 
        metadata_value_id,
        metadata_field_id,
        text_value,
        text_lang,
        place,
        authority,
        confidence,
        dspace_object_id,
        {{ dbt_date.today() }} as load_datetime
    FROM {{ source('dspace', 'metadatavalue') }}
)

SELECT * FROM base