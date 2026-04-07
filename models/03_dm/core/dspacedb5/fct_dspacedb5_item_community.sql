{{ config(materialized='table') }}

WITH publication AS (
    SELECT *
    FROM {{ ref('fct_dspacedb5_item_publication') }}
),
item_collection AS (
    SELECT *
    FROM {{ ref('brg_dspacedb5_item_collection') }}
),
collection_dim AS (
    SELECT *
    FROM {{ ref('dim_dspacedb5_collection') }}
),
collection_community AS (
    SELECT
        collection_hk,
        community_hk,
        ROW_NUMBER() OVER (
            PARTITION BY collection_hk
            ORDER BY extract_datetime DESC, load_datetime DESC, community_collection_id DESC
        ) AS rn
    FROM {{ ref('stg_dspacedb5_community2collection') }}
),
collection_community_dedup AS (
    SELECT
        collection_hk,
        community_hk
    FROM collection_community
    WHERE rn = 1
),
community_dim AS (
    SELECT *
    FROM {{ ref('dim_dspacedb5_community') }}
),
final AS (
    SELECT
        p.item_hk,
        p.item_id,
        ic.collection_hk,
        ic.collection_id,
        cd.collection_title,
        cc.community_hk,
        cmd.community_id,
        cmd.community_title,
        cmd.parent_community_hk,
        cmd.parent_community_id,
        cmd.parent_community_title,
        cmd.root_community_hk,
        cmd.root_community_id,
        cmd.root_community_title,
        cmd.community_depth,
        cmd.community_path_ids,
        cmd.community_path_titles,
        p.owning_collection,
        p.owningcollection_hk,
        ic.collections_count,
        ic.collection_hk = p.owningcollection_hk AS is_owning_collection,
        p.source_label,
        p.institution_ror
    FROM publication p
    JOIN item_collection ic
        USING (item_hk)
    LEFT JOIN collection_dim cd
        USING (collection_hk)
    LEFT JOIN collection_community_dedup cc
        USING (collection_hk)
    LEFT JOIN community_dim cmd
        USING (community_hk)
)

SELECT * FROM final
