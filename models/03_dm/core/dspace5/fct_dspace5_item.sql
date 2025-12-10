{{ config(materialized = 'table') }}

WITH item AS (
    SELECT 
        hub_i.item_id,
        sat_i.in_archive,
        sat_i.withdrawn,
        sat_i.discoverable,
        hub_i.item_hk,
        lnk_i_col.owningcollection_hk
    FROM {{ref('hub_dspace5_item')}} hub_i
    INNER JOIN {{ latest_satellite(ref('sat_dspace5_item'), 'item_hk') }} AS sat_i 
        ON sat_i.item_hk = hub_i.item_hk    
    INNER JOIN {{ref('link_dspace5_item_owningcollection')}} lnk_i_col ON
        lnk_i_col.item_hk = hub_i.item_hk
)

SELECT * FROM item
