{{ config(materialized='table') }}

WITH base AS (
    SELECT
        item_uuid,
        COUNT(*)
    FROM {{ ref('fct_dspace_item') }} fct
    LEFT JOIN {{ref('brg_dspace_item_metadatavalue')}} USING (item_hk)
    LEFT JOIN {{ref('dim_dspace_metadatavalue')}} USING (metadatavalue_hk)
    WHERE 
        (metadatafield_fullname = 'dcterms.identifier.other' OR 
        metadatafield_fullname is null) AND 
        (text_value ilike '%doi%' OR
        text_value is null)
    GROUP BY 1
    ORDER BY COUNT(*) DESC
),

has_doi AS (
    SELECT 
        *,
        CASE WHEN count = 0 THEN False ELSE True END AS has_doi 
    FROM base
)

SELECT * FROM base WHERE count = 0