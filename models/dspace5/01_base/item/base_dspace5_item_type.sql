{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        i.item_id,
        text_value as type
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'dc' AND 
        i.element = 'type' AND 
        i.qualifier is NULL
)

SELECT * FROM base