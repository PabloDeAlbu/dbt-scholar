{{ config(materialized = 'table') }}

WITH item AS (
    SELECT
        hub_i.item_id,
        sat_i.in_archive,
        sat_i.withdrawn,
        sat_i.discoverable,
        hub_i.item_hk,
        hub_i.load_datetime,
        ROW_NUMBER() OVER (
            PARTITION BY hub_i.item_hk
            ORDER BY hub_i.load_datetime DESC
        ) AS rn_item
    FROM {{ ref('hub_dspace5_item') }} hub_i
    INNER JOIN {{ latest_satellite(ref('sat_dspace5_item'), 'item_hk') }} AS sat_i
        ON sat_i.item_hk = hub_i.item_hk
),
item_dedup AS (
    SELECT item_id, in_archive, withdrawn, discoverable, item_hk
    FROM item
    WHERE rn_item = 1
),
owning_collection AS (
    SELECT
        lnk.item_hk,
        lnk.owningcollection_hk,
        ROW_NUMBER() OVER (
            PARTITION BY lnk.item_hk
            ORDER BY lnk.load_datetime DESC
        ) AS rn_oc
    FROM {{ ref('link_dspace5_item_owningcollection') }} lnk
),
owning_collection_dedup AS (
    SELECT item_hk, owningcollection_hk
    FROM owning_collection
    WHERE rn_oc = 1
),
collections_count AS (
    SELECT
        lnk.item_hk,
        COUNT(DISTINCT lnk.collection_hk) AS collections_count
    FROM {{ ref('link_dspace5_collection_item') }} lnk
    GROUP BY lnk.item_hk
)

SELECT
    i.item_id,
    i.in_archive,
    i.withdrawn,
    i.discoverable,
    oc.owningcollection_hk,
    COALESCE(cc.collections_count, 0) AS collections_count,
    i.item_hk
FROM item_dedup i
JOIN owning_collection_dedup oc USING (item_hk)
JOIN collections_count cc USING (item_hk)
