{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        i.item_id,
        text_value as title,
        text_lang as title_lang
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'dc' AND 
        i.element = 'title' AND 
        i.qualifier is NULL
)

SELECT * FROM base