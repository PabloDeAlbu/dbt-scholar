{{ config(materialized = "table") }}

WITH base AS (
    SELECT 
        mv.uuid,
        CONCAT('10.', split_part(text_value, '10.', 2))  as doi,
        load_datetime
    FROM {{ref('mid_item_metadatavalue_dspacedb')}} mv 
    WHERE 
        mv.short_id = 'dcterms' AND 
        mv.element = 'identifier' AND 
        mv.qualifier = 'other' AND
        mv.text_value like '%10.%'
)

SELECT * FROM base
