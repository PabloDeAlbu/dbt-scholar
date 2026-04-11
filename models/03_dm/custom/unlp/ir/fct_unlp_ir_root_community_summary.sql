{{ config(materialized='table') }}

WITH item_community AS (
    SELECT *
    FROM {{ ref('fct_unlp_ir_item_community') }}
),

item_publication AS (
    SELECT
        item_hk,
        discoverable,
        dc_date_issued,
        owning_root_community_hk,
        owning_root_community_id,
        owning_root_community_title
    FROM {{ ref('fct_unlp_ir_item_publication') }}
),

final AS (
    SELECT
        COALESCE(ip.owning_root_community_hk, ic.root_community_hk) AS root_community_hk,
        COALESCE(ip.owning_root_community_id, ic.root_community_id) AS root_community_id,
        COALESCE(ip.owning_root_community_title, ic.root_community_title, '!UNKNOWN') AS root_community_title,
        COUNT(DISTINCT ic.item_hk) AS item_count,
        COUNT(DISTINCT ic.item_hk) FILTER (WHERE ic.is_owning_collection) AS item_count_owning_collection,
        COUNT(DISTINCT ic.collection_hk) AS collection_count,
        COUNT(DISTINCT ic.community_hk) AS community_count,
        COUNT(DISTINCT ic.item_hk) FILTER (WHERE ip.discoverable) AS discoverable_item_count,
        COUNT(DISTINCT ic.item_hk) FILTER (WHERE ip.dc_date_issued IS NOT NULL) AS issued_item_count,
        MIN(ip.dc_date_issued) AS first_issued_date,
        MAX(ip.dc_date_issued) AS last_issued_date
    FROM item_community AS ic
    LEFT JOIN item_publication AS ip
        USING (item_hk)
    GROUP BY 1, 2, 3
)

SELECT * FROM final
