{{ config(materialized = 'table') }}

-- ER simple: comunidad y su padre inmediato (si existe), con títulos
WITH community_titles AS (
    SELECT
        community_hk,
        text_value AS title
    FROM {{ ref('er_dspace5_community_metadatavalue') }}
    WHERE short_id = 'dc'
      AND element = 'title'
      AND qualifier IS NULL
),
community_parent AS (
    SELECT
        lnk.child_comm_hk AS community_hk,
        lnk.parent_comm_hk,
        ROW_NUMBER() OVER (
            PARTITION BY lnk.child_comm_hk
            ORDER BY lnk.load_datetime DESC
        ) AS rn
    FROM {{ ref('link_dspace5_community_community') }} lnk
),
community_parent_dedup AS (
    SELECT community_hk, parent_comm_hk
    FROM community_parent
    WHERE rn = 1
)

SELECT
    cp.community_hk,
    cp.parent_comm_hk,
    ct.title AS community_title,
    ct_parent.title AS parent_comm_title
FROM community_parent_dedup cp
LEFT JOIN community_titles ct
    ON ct.community_hk = cp.community_hk
LEFT JOIN community_titles ct_parent
    ON ct_parent.community_hk = cp.parent_comm_hk
