{{ config(
    materialized='table',
    indexes=[
        {'columns': ['journal_id'], 'unique': true}
    ],
    post_hook=["analyze {{ this }}"]
) }}

SELECT
    community_id AS journal_id,
    community_title AS journal_title,
    community_title LIKE '%[Publicación cerrada]%' AS is_closed,
    root_community_id,
    root_community_title,
    community_path_ids AS journal_path_ids,
    community_path_titles AS journal_path_titles
FROM {{ ref('dim_unlp_sedicidb_community') }}
WHERE root_community_title = 'Revistas'
  AND community_depth = 1
