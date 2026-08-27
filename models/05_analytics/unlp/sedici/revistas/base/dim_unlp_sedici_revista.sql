{{ config(
    materialized='table',
    indexes=[
        {'columns': ['journal_id'], 'unique': true}
    ],
    post_hook=["analyze {{ this }}"]
) }}

WITH community_handle AS (
    SELECT
        resource_id AS community_id,
        MIN(NULLIF(BTRIM(handle), '')) AS journal_handle
    FROM {{ source('sedicidb', 'handle') }}
    WHERE resource_type_id = 4
    GROUP BY resource_id
)

SELECT
    community.community_id AS journal_id,
    community.community_title AS journal_title,
    handle.journal_handle,
    community.community_title LIKE '%[Publicación cerrada]%' AS is_closed,
    community.root_community_id,
    community.root_community_title,
    community.community_path_ids AS journal_path_ids,
    community.community_path_titles AS journal_path_titles
FROM {{ ref('dim_unlp_sedicidb_community') }} AS community
LEFT JOIN community_handle AS handle
    USING (community_id)
WHERE community.root_community_title = 'Revistas'
  AND community.community_depth = 1
