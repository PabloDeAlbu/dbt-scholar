{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        mv.uuid,
        text_value as title,
        text_lang as title_lang,
        load_datetime
    FROM {{ref('mid_item_metadatavalue_dspacedb')}} mv 
    WHERE 
        mv.short_id = 'dc' AND 
        mv.element = 'title' AND 
        mv.qualifier is NULL
)

SELECT * FROM base