{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        mv.uuid,
        text_value as type,
        load_datetime
    FROM {{ref('mid_item_metadatavalue_dspacedb')}} mv 
    WHERE 
        mv.short_id = 'dc' AND 
        mv.element = 'type' AND
        mv.qualifier IS NULL
)

SELECT * FROM base