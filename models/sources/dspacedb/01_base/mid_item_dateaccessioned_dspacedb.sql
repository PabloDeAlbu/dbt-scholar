{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        mv.uuid,
        {{ dbt_date.convert_timezone(str_to_date("text_value")) }} as dateissued,
        load_datetime
    FROM {{ref('mid_item_metadatavalue_dspacedb')}} mv 
    WHERE 
        mv.short_id = 'dc' AND 
        mv.element = 'date' AND 
        qualifier = 'accessioned'       
)

SELECT * FROM base