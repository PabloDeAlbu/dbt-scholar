{{ config(materialized='view') }}

SELECT
    value.dspace_object_id AS item_id,
    value.metadata_value_id,
    value.metadata_field_id,
    schema_registry.short_id || '.' || field.element || CASE
        WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
            THEN '.' || BTRIM(field.qualifier)
        ELSE ''
    END AS metadatafield_fullname,
    value.text_value AS value_raw,
    NULLIF({{ clean_text("REPLACE(value.text_value, '|', ' ')") }}, '') AS value,
    value.text_lang AS text_lang_raw,
    NULLIF(LOWER(BTRIM(value.text_lang)), '') AS text_lang,
    value.authority AS authority_raw,
    NULLIF(BTRIM(value.authority), '') AS authority,
    value.confidence,
    value.place
FROM {{ source('cicdigital', 'metadatavalue') }} AS value
INNER JOIN {{ source('cicdigital', 'item') }} AS item
    ON item.uuid = value.dspace_object_id
INNER JOIN {{ source('cicdigital', 'metadatafieldregistry') }} AS field
    USING (metadata_field_id)
INNER JOIN {{ source('cicdigital', 'metadataschemaregistry') }} AS schema_registry
    USING (metadata_schema_id)
