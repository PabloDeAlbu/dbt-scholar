{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        mv.uuid,
        text_value as handle,
        load_datetime
    FROM {{ref('mid_item_metadatavalue_dspacedb')}} mv 
    WHERE 
        mv.short_id = 'dc' AND 
        mv.element = 'identifier' AND 
        mv.qualifier = 'uri' AND
        mv.text_value like '%handle%'
)

SELECT * FROM base