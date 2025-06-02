{{ config(materialized='table') }}

WITH base AS (
    SELECT
    mf.metadata_field_id,
    CONCAT(ms.short_id, '.', mf.element, '.', COALESCE(mf.qualifier, '')) AS metadato,
    COUNT(mv.*) AS cantidad_usos,
    COUNT(DISTINCT mv.dspace_object_id) AS items_que_lo_usaron,
    COUNT(*) FILTER (WHERE mv.text_value IS NULL OR mv.text_value = '') AS vacios
    FROM {{ref('stg_dspace_metadatafieldregistry')}} mf
    JOIN {{ref('stg_dspace_metadataschemaregistry')}} ms ON mf.metadata_schema_id = ms.metadata_schema_id
    LEFT JOIN {{ref('stg_dspace_metadatavalue')}} mv ON mv.metadata_field_id = mf.metadata_field_id
    GROUP BY mf.metadata_field_id, ms.short_id, mf.element, mf.qualifier
)

SELECT * FROM base