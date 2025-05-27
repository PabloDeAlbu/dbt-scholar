{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        i.item_id,
        {{ dbt_date.convert_timezone("text_value") }} as dateavailable
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'dc' AND 
        i.element = 'date' AND 
        i.qualifier = 'available'
)

SELECT * FROM base