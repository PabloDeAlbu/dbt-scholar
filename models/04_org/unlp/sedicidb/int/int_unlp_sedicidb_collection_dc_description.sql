{{ config(materialized='view') }}

SELECT
    collection.collection_id,
    value.metadata_value_id,
    value.text_value AS value_raw,
    NULLIF({{ clean_text('value.text_value') }}, '') AS value
FROM {{ source('sedicidb', 'collection') }} AS collection
INNER JOIN {{ source('sedicidb', 'metadatavalue') }} AS value
    ON value.resource_id = collection.collection_id
   AND value.resource_type_id = 3
INNER JOIN {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    USING (metadata_field_id)
INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
    USING (metadata_schema_id)
WHERE schema_registry.short_id = 'dc'
  AND field.element = 'description'
  AND NULLIF(BTRIM(field.qualifier), '') IS NULL
  AND NULLIF({{ clean_text('value.text_value') }}, '') IS NOT NULL
