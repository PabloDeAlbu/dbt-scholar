{{ config(materialized='table') }}

WITH object_title AS (
    SELECT
        value.dspace_object_id AS object_id,
        value.metadata_value_id,
        NULLIF({{ clean_text("REPLACE(value.text_value, '|', ' ')") }}, '') AS value,
        ROW_NUMBER() OVER (
            PARTITION BY value.dspace_object_id
            ORDER BY value.metadata_value_id DESC
        ) AS value_rank
    FROM {{ source('cicdigital', 'metadatavalue') }} AS value
    INNER JOIN {{ source('cicdigital', 'metadatafieldregistry') }} AS field
        USING (metadata_field_id)
    INNER JOIN {{ source('cicdigital', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
    WHERE schema_registry.short_id = 'dc'
      AND field.element = 'title'
      AND NULLIF(BTRIM(field.qualifier), '') IS NULL
      AND NULLIF(BTRIM(value.text_value), '') IS NOT NULL
),

collection_community AS (
    SELECT
        collection_id,
        community_id,
        ROW_NUMBER() OVER (
            PARTITION BY collection_id
            ORDER BY community_id
        ) AS community_rank
    FROM {{ source('cicdigital', 'community2collection') }}
),

final AS (
    SELECT
        collection.uuid AS collection_id,
        collection_title.value AS collection_title,
        relation.community_id,
        community_title.value AS community_title
    FROM {{ source('cicdigital', 'collection') }} AS collection
    LEFT JOIN object_title AS collection_title
        ON collection_title.object_id = collection.uuid
       AND collection_title.value_rank = 1
    LEFT JOIN collection_community AS relation
        ON relation.collection_id = collection.uuid
       AND relation.community_rank = 1
    LEFT JOIN object_title AS community_title
        ON community_title.object_id = relation.community_id
       AND community_title.value_rank = 1
)

SELECT * FROM final
