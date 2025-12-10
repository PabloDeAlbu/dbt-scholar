{{ config(materialized='table') }}

WITH final as (
    SELECT 
        text_value,
        authority,
        CASE WHEN COALESCE(authority, '-') <> '-' THEN TRUE ELSE FALSE END AS has_authority
    FROM {{ref('dim_dspace_metadatavalue')}} dim
    WHERE short_id = 'dcterms' AND element = 'creator' AND qualifier = 'author'
)

SELECT * FROM final
