{{ config(materialized='table') }}

WITH RECURSIVE community_base AS (
    -- FIXME: move the community node base to a core bridge/dimension on top of DV instead of reading it from stg.
    SELECT
        community_hk,
        community_bk,
        community_id,
        source_label,
        institution_ror,
        extract_datetime,
        load_datetime
    FROM {{ ref('stg_dspacedb5_community') }}
),
title_candidates AS (
    SELECT
        c.community_hk,
        mv.text_value AS community_title,
        ROW_NUMBER() OVER (
            PARTITION BY c.community_hk
            ORDER BY
                (mv.text_lang IS NULL) DESC,
                mv.text_lang,
                mv.place,
                mv.text_value,
                mv.metadatavalue_hk
        ) AS rn
    FROM community_base c
    JOIN {{ ref('brg_dspacedb5_community_metadatavalue') }} mv
        USING (community_hk)
    WHERE mv.short_id = 'dc'
      AND mv.element = 'title'
      AND COALESCE(mv.qualifier, '') IN ('', '!UNKNOWN')
      AND mv.source_label = c.source_label
      AND mv.institution_ror = c.institution_ror
      AND mv.text_value IS NOT NULL
      AND mv.text_value <> '!UNKNOWN'
),
community_nodes AS (
    SELECT
        c.community_hk,
        c.community_id,
        tc.community_title,
        c.source_label,
        c.institution_ror,
        c.extract_datetime,
        c.load_datetime
    FROM community_base c
    LEFT JOIN title_candidates tc
        ON c.community_hk = tc.community_hk
       AND tc.rn = 1
),
community_parent AS (
    -- FIXME: move the parent-child hierarchy to a core bridge on top of DV instead of reading it from stg.
    SELECT
        child_comm_hk AS community_hk,
        parent_comm_hk,
        ROW_NUMBER() OVER (
            PARTITION BY child_comm_hk
            ORDER BY extract_datetime DESC, load_datetime DESC, community_community_id DESC
        ) AS rn
    FROM {{ ref('stg_dspacedb5_community2community') }}
),
community_parent_dedup AS (
    SELECT
        community_hk,
        parent_comm_hk
    FROM community_parent
    WHERE rn = 1
),
lineage AS (
    SELECT
        n.community_hk AS leaf_community_hk,
        n.community_hk AS ancestor_community_hk,
        n.community_id AS ancestor_community_id,
        n.community_title AS ancestor_community_title,
        p.parent_comm_hk,
        0 AS depth_from_leaf,
        ARRAY[n.community_hk] AS visited_hk
    FROM community_nodes n
    LEFT JOIN community_parent_dedup p
        ON n.community_hk = p.community_hk

    UNION ALL

    SELECT
        l.leaf_community_hk,
        n.community_hk AS ancestor_community_hk,
        n.community_id AS ancestor_community_id,
        n.community_title AS ancestor_community_title,
        p.parent_comm_hk,
        l.depth_from_leaf + 1 AS depth_from_leaf,
        ARRAY_APPEND(l.visited_hk, n.community_hk) AS visited_hk
    FROM lineage l
    JOIN community_nodes n
        ON l.parent_comm_hk = n.community_hk
    LEFT JOIN community_parent_dedup p
        ON n.community_hk = p.community_hk
    WHERE NOT (n.community_hk = ANY(l.visited_hk))
),
lineage_agg AS (
    SELECT
        leaf_community_hk AS community_hk,
        MAX(depth_from_leaf) AS community_depth,
        STRING_AGG(
            COALESCE(ancestor_community_id::text, '!UNKNOWN'),
            ' > '
            ORDER BY depth_from_leaf DESC
        ) AS community_path_ids,
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
        ancestor_community_title AS parent_community_title
    FROM lineage
    WHERE depth_from_leaf = 1
),
root_community AS (
    SELECT DISTINCT ON (leaf_community_hk)
        leaf_community_hk AS community_hk,
        ancestor_community_hk AS root_community_hk,
        ancestor_community_id AS root_community_id,
        ancestor_community_title AS root_community_title
    FROM lineage
    ORDER BY leaf_community_hk, depth_from_leaf DESC, ancestor_community_id
),
final AS (
    SELECT
        n.community_hk,
        n.community_id,
        n.community_title,
        ip.parent_community_hk,
        ip.parent_community_id,
        ip.parent_community_title,
        rc.root_community_hk,
        rc.root_community_id,
        rc.root_community_title,
        la.community_depth,
        la.community_path_ids,
        la.community_path_titles,
        n.source_label,
        n.institution_ror,
        n.extract_datetime,
        n.load_datetime
    FROM community_nodes n
    LEFT JOIN immediate_parent ip
        USING (community_hk)
    LEFT JOIN root_community rc
        USING (community_hk)
    LEFT JOIN lineage_agg la
        USING (community_hk)
)

SELECT * FROM final
