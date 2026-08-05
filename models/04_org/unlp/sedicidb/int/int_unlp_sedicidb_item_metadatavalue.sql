{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_value_id'], 'unique': true},
        {'columns': ['item_id', 'metadata_field_id']},
        {'columns': ['metadatafield_fullname', 'item_id']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH metadatafield AS (
    SELECT
        field.metadata_field_id,
        field.metadata_schema_id,
        schema_registry.short_id AS metadata_schema,
        field.element,
        NULLIF(BTRIM(field.qualifier), '') AS qualifier,
        schema_registry.short_id || '.' || field.element || CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN '.' || BTRIM(field.qualifier)
            ELSE ''
        END AS metadatafield_fullname
    FROM {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
),

final AS (
    SELECT
        value.resource_id AS item_id,
        value.metadata_value_id,
        value.metadata_field_id,
        field.metadata_schema_id,
        field.metadata_schema,
        field.element,
        field.qualifier,
        field.metadatafield_fullname,
        value.text_value AS text_value_raw,
        NULLIF({{ clean_text('value.text_value') }}, '') AS text_value,
        value.text_lang AS text_lang_raw,
        NULLIF(LOWER(BTRIM(value.text_lang)), '') AS text_lang,
        value.authority AS authority_raw,
        NULLIF(BTRIM(value.authority), '') AS authority_uri,
        LOWER(SUBSTRING(BTRIM(value.authority) FROM '^https?://([^/]+)')) AS authority_host,
        NULLIF(
            SUBSTRING(BTRIM(value.authority) FROM '^https?://[^/]+/?(.*)$'),
            ''
        ) AS authority_path,
        value.confidence,
        value.place
    FROM {{ source('sedicidb', 'metadatavalue') }} AS value
    INNER JOIN {{ source('sedicidb', 'item') }} AS item
        ON item.item_id = value.resource_id
    INNER JOIN metadatafield AS field
        USING (metadata_field_id)
    WHERE value.resource_type_id = 2
)

SELECT *
FROM final
