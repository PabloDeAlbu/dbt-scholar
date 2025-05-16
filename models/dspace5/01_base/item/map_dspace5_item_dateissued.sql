{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        i.item_id,
        {{ dbt_date.convert_timezone(str_to_date("text_value")) }} as dateissued,
        text_lang
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'dc' AND 
        i.element = 'date' AND 
        i.qualifier = 'issued'
)

SELECT * FROM base
--WHERE LEFT(CAST(dateissued AS TEXT), 4) = '9999'
