{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        i.item_id,
        CONCAT('10.', split_part(text_value, '10.', 2))  as doi
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'dc' AND 
        i.element = 'identifier' AND 
        i.qualifier = 'uri' AND
        i.text_value like '%10.%'
)

SELECT * FROM base