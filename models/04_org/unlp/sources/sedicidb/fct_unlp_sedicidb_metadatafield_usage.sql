{{ config(
    materialized='table',
    indexes=[
        {'columns': ['metadata_field_id'], 'unique': true},
        {'columns': ['metadatafield_fullname'], 'unique': true},
        {'columns': ['metadata_value_count']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

-- ¿Qué metadatafields están registrados en SEDICIDB?
WITH metadatafield AS (
    SELECT
        field.metadata_field_id,
        field.metadata_schema_id,
        schema_registry.short_id AS metadata_schema,
        field.element,
        NULLIF(BTRIM(field.qualifier), '') AS qualifier,
        CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN schema_registry.short_id || '.' || field.element || '.' || BTRIM(field.qualifier)
            ELSE schema_registry.short_id || '.' || field.element
        END AS metadatafield_fullname,
        field.scope_note
    FROM {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
),

-- ¿Cuántos valores utilizan cada metadatafield en todo el repositorio?
metadatafield_usage AS (
    SELECT
        metadata_field_id,
        COUNT(*)::bigint AS metadata_value_count
    FROM {{ source('sedicidb', 'metadatavalue') }}
    GROUP BY metadata_field_id
),

final AS (
    SELECT
        field.metadata_field_id,
        field.metadata_schema_id,
        field.metadata_schema,
        field.element,
        field.qualifier,
        field.metadatafield_fullname,
        field.scope_note,
        COALESCE(usage.metadata_value_count, 0) AS metadata_value_count
    FROM metadatafield AS field
    LEFT JOIN metadatafield_usage AS usage
        USING (metadata_field_id)
)

SELECT *
FROM final
