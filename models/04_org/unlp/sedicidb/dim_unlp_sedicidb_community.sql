{{ config(
    materialized='table',
    indexes=[
        {'columns': ['community_id'], 'unique': true},
        {'columns': ['parent_community_id']},
        {'columns': ['root_community_id']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH RECURSIVE metadatafield AS (
    SELECT
        field.metadata_field_id,
        schema_registry.short_id || '.' || field.element || CASE
            WHEN NULLIF(BTRIM(field.qualifier), '') IS NOT NULL
                THEN '.' || BTRIM(field.qualifier)
            ELSE ''
        END AS metadatafield_fullname
    FROM {{ source('sedicidb', 'metadatafieldregistry') }} AS field
    INNER JOIN {{ source('sedicidb', 'metadataschemaregistry') }} AS schema_registry
        USING (metadata_schema_id)
),

title_value AS (
    SELECT
        value.resource_id AS community_id,
        value.metadata_value_id,
        NULLIF({{ clean_text('value.text_value') }}, '') AS community_title,
        NULLIF(LOWER(BTRIM(value.text_lang)), '') AS text_lang,
        value.place
    FROM {{ source('sedicidb', 'metadatavalue') }} AS value
    INNER JOIN metadatafield AS field
        USING (metadata_field_id)
    WHERE value.resource_type_id = 4
      AND field.metadatafield_fullname = 'dc.title'
),

title_ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY community_id
            ORDER BY
                (text_lang IS NULL) DESC,
                text_lang,
                place NULLS LAST,
                metadata_value_id
        ) AS title_rank
    FROM title_value
    WHERE community_title IS NOT NULL
),

community_node AS (
    SELECT
        community.community_id,
        title.community_title
    FROM {{ source('sedicidb', 'community') }} AS community
    LEFT JOIN title_ranked AS title
        ON title.community_id = community.community_id
       AND title.title_rank = 1
),

community_parent AS (
    SELECT DISTINCT ON (relation.child_comm_id)
        relation.child_comm_id AS community_id,
        relation.parent_comm_id AS parent_community_id
    FROM {{ source('sedicidb', 'community2community') }} AS relation
    ORDER BY relation.child_comm_id, relation.parent_comm_id
),

lineage AS (
    SELECT
        community.community_id AS leaf_community_id,
        community.community_id AS ancestor_community_id,
        community.community_title AS ancestor_community_title,
        parent.parent_community_id,
        0 AS depth_from_leaf,
        ARRAY[community.community_id] AS visited_community_ids
    FROM community_node AS community
    LEFT JOIN community_parent AS parent
        USING (community_id)

    UNION ALL

    SELECT
        lineage.leaf_community_id,
        ancestor.community_id AS ancestor_community_id,
        ancestor.community_title AS ancestor_community_title,
        parent.parent_community_id,
        lineage.depth_from_leaf + 1 AS depth_from_leaf,
        ARRAY_APPEND(lineage.visited_community_ids, ancestor.community_id)
            AS visited_community_ids
    FROM lineage
    INNER JOIN community_node AS ancestor
        ON ancestor.community_id = lineage.parent_community_id
    LEFT JOIN community_parent AS parent
        ON parent.community_id = ancestor.community_id
    WHERE NOT ancestor.community_id = ANY(lineage.visited_community_ids)
),

lineage_aggregate AS (
    SELECT
        leaf_community_id AS community_id,
        MAX(depth_from_leaf)::integer AS community_depth,
        STRING_AGG(
            ancestor_community_id::text,
            ' > '
            ORDER BY depth_from_leaf DESC
        ) AS community_path_ids,
        STRING_AGG(
            COALESCE(ancestor_community_title, '!UNKNOWN'),
            ' > '
            ORDER BY depth_from_leaf DESC
        ) AS community_path_titles
    FROM lineage
    GROUP BY leaf_community_id
),

root_community AS (
    SELECT DISTINCT ON (leaf_community_id)
        leaf_community_id AS community_id,
        ancestor_community_id AS root_community_id,
        ancestor_community_title AS root_community_title
    FROM lineage
    ORDER BY leaf_community_id, depth_from_leaf DESC, ancestor_community_id
),

final AS (
    SELECT
        community.community_id,
        community.community_title,
        parent.parent_community_id,
        parent_node.community_title AS parent_community_title,
        root.root_community_id,
        root.root_community_title,
        path.community_depth,
        path.community_path_ids,
        path.community_path_titles
    FROM community_node AS community
    LEFT JOIN community_parent AS parent
        USING (community_id)
    LEFT JOIN community_node AS parent_node
        ON parent_node.community_id = parent.parent_community_id
    LEFT JOIN root_community AS root
        ON root.community_id = community.community_id
    LEFT JOIN lineage_aggregate AS path
        ON path.community_id = community.community_id
)

SELECT *
FROM final
