{{ config(
    materialized='table',
    indexes=[
        {'columns': ['community_hk'], 'unique': true},
        {'columns': ['community_uuid']},
        {'columns': ['parent_community_hk']},
        {'columns': ['root_community_hk']},
        {'columns': ['institution_ror', 'base_url']}
    ],
    post_hook=[
        "analyze {{ this }}"
    ]
) }}

WITH RECURSIVE community_node AS (
    SELECT
        community_hk,
        community_id,
        community_uuid,
        community_title,
        community_url,
        base_url,
        source_label,
        institution_ror
    FROM {{ ref('dim_dspacedb_community') }}
),

parent_observed AS (
    SELECT
        child_comm_hk AS community_hk,
        parent_comm_hk AS parent_community_hk,
        ROW_NUMBER() OVER (
            PARTITION BY child_comm_hk
            ORDER BY load_datetime DESC, community_community_hk DESC
        ) AS observation_rank
    FROM {{ ref('link_dspacedb_community_community') }}
),

current_parent AS (
    SELECT
        community_hk,
        parent_community_hk
    FROM parent_observed
    WHERE observation_rank = 1
),

lineage AS (
    SELECT
        node.community_hk AS leaf_community_hk,
        node.community_hk AS ancestor_community_hk,
        node.community_id AS ancestor_community_id,
        node.community_uuid AS ancestor_community_uuid,
        node.community_title AS ancestor_community_title,
        parent.parent_community_hk,
        0 AS depth_from_leaf,
        ARRAY[node.community_hk] AS visited_community_hks
    FROM community_node AS node
    LEFT JOIN current_parent AS parent
        USING (community_hk)

    UNION ALL

    SELECT
        lineage.leaf_community_hk,
        ancestor.community_hk AS ancestor_community_hk,
        ancestor.community_id AS ancestor_community_id,
        ancestor.community_uuid AS ancestor_community_uuid,
        ancestor.community_title AS ancestor_community_title,
        parent.parent_community_hk,
        lineage.depth_from_leaf + 1 AS depth_from_leaf,
        ARRAY_APPEND(lineage.visited_community_hks, ancestor.community_hk)
            AS visited_community_hks
    FROM lineage
    INNER JOIN community_node AS ancestor
        ON lineage.parent_community_hk = ancestor.community_hk
    LEFT JOIN current_parent AS parent
        ON ancestor.community_hk = parent.community_hk
    WHERE NOT ancestor.community_hk = ANY(lineage.visited_community_hks)
),

lineage_aggregate AS (
    SELECT
        leaf_community_hk AS community_hk,
        MAX(depth_from_leaf) AS community_depth,
        STRING_AGG(
            COALESCE(ancestor_community_id::text, '!UNKNOWN'),
            ' > '
            ORDER BY depth_from_leaf DESC
        ) AS community_path_ids,
        STRING_AGG(
            ancestor_community_uuid::text,
            ' > '
            ORDER BY depth_from_leaf DESC
        ) AS community_path_uuids,
        STRING_AGG(
            COALESCE(ancestor_community_title, '!UNKNOWN'),
            ' > '
            ORDER BY depth_from_leaf DESC
        ) AS community_path_titles
    FROM lineage
    GROUP BY leaf_community_hk
),

immediate_parent AS (
    SELECT
        leaf_community_hk AS community_hk,
        ancestor_community_hk AS parent_community_hk,
        ancestor_community_id AS parent_community_id,
        ancestor_community_uuid AS parent_community_uuid,
        ancestor_community_title AS parent_community_title
    FROM lineage
    WHERE depth_from_leaf = 1
),

root_community AS (
    SELECT DISTINCT ON (leaf_community_hk)
        leaf_community_hk AS community_hk,
        ancestor_community_hk AS root_community_hk,
        ancestor_community_id AS root_community_id,
        ancestor_community_uuid AS root_community_uuid,
        ancestor_community_title AS root_community_title
    FROM lineage
    ORDER BY
        leaf_community_hk,
        depth_from_leaf DESC,
        ancestor_community_uuid
),

final AS (
    SELECT
        node.community_hk,
        node.community_id,
        node.community_uuid,
        node.community_title,
        node.community_url,
        parent.parent_community_hk,
        parent.parent_community_id,
        parent.parent_community_uuid,
        parent.parent_community_title,
        root.root_community_hk,
        root.root_community_id,
        root.root_community_uuid,
        root.root_community_title,
        path.community_depth,
        path.community_path_ids,
        path.community_path_uuids,
        path.community_path_titles,
        node.base_url,
        node.source_label,
        node.institution_ror
    FROM community_node AS node
    LEFT JOIN immediate_parent AS parent
        USING (community_hk)
    LEFT JOIN root_community AS root
        USING (community_hk)
    LEFT JOIN lineage_aggregate AS path
        USING (community_hk)
)

SELECT *
FROM final
