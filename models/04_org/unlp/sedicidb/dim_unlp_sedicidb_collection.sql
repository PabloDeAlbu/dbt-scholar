{{ config(
    materialized='table',
    indexes=[
        {'columns': ['collection_id'], 'unique': true},
        {'columns': ['community_id']},
        {'columns': ['root_community_id']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH title_value AS (
    SELECT
        collection.collection_id,
        value.metadata_value_id,
        NULLIF({{ clean_text('value.text_value') }}, '') AS collection_title,
        NULLIF(LOWER(BTRIM(value.text_lang)), '') AS text_lang,
        value.place
    FROM {{ source('sedicidb', 'collection') }} AS collection
    LEFT JOIN {{ source('sedicidb', 'metadatavalue') }} AS value
        ON value.resource_id = collection.collection_id
       AND value.resource_type_id = 3
    LEFT JOIN {{ source('sedicidb', 'metadatafieldregistry') }} AS field
        USING (metadata_field_id)
    LEFT JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
    WHERE schema_registry.short_id = 'dc'
      AND field.element = 'title'
      AND NULLIF(BTRIM(field.qualifier), '') IS NULL
),

title_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY collection_id
            ORDER BY
                (text_lang IS NULL) DESC,
                text_lang,
                place NULLS LAST,
                metadata_value_id
        ) AS title_rank
    FROM title_value
    WHERE collection_title IS NOT NULL
),

collection_community AS (
    SELECT DISTINCT ON (relation.collection_id)
        relation.collection_id,
        relation.community_id
    FROM {{ source('sedicidb', 'community2collection') }} AS relation
    ORDER BY relation.collection_id, relation.community_id
),

final AS (
    SELECT
        collection.collection_id,
        title.collection_title,
        relation.community_id,
        community.community_title,
        community.root_community_id,
        community.root_community_title,
        community.community_depth,
        community.community_path_ids,
        community.community_path_titles
    FROM {{ source('sedicidb', 'collection') }} AS collection
    LEFT JOIN title_ranked AS title
        ON title.collection_id = collection.collection_id
       AND title.title_rank = 1
    LEFT JOIN collection_community AS relation
        ON relation.collection_id = collection.collection_id
    LEFT JOIN {{ ref('dim_unlp_sedicidb_community') }} AS community
        ON community.community_id = relation.community_id
)

SELECT *
FROM final
