WITH base AS (
    SELECT 
        hub_i.item_uuid,
        hub_i.item_hk
    FROM {{ref('hub_dspace_item')}} hub_i
)

SELECT * FROM base