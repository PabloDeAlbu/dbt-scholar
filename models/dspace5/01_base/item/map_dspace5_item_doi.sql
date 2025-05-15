{{ config(materialized = "table") }}

WITH uri AS (
    SELECT 
        i.item_id,
        text_value as doi
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'dc' AND 
        i.element = 'identifier' AND 
        i.qualifier = 'uri' AND
        i.text_value like '%10.%'
),

other AS (
    SELECT 
        i.item_id,
        text_value as doi
    FROM {{ref('base_dspace5_item_metadatavalue')}} i 
    WHERE 
        i.short_id = 'sedici' AND 
        i.element = 'identifier' AND 
        i.qualifier = 'other' AND
        i.text_value like '%10.%'
),

dois AS (
    SELECT * FROM uri 
    UNION
    SELECT * FROM other
),

normalized_dois AS (
    SELECT 
        item_id,
        (regexp_matches(doi, '10\.\d{4,}[^\s/]+/[^\s]+'))[1] as doi
    FROM dois
    WHERE doi ~ '10\.\d{4,}'
)

SELECT * FROM normalized_dois 
WHERE doi IS NOT NULL
ORDER BY doi DESC