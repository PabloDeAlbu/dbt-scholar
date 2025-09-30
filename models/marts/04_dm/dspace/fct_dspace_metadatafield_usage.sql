{{ config(materialized='table') }}

WITH usage_count AS (
    SELECT
        metadatafield_hk,
        COUNT(*) AS total_uses
    FROM {{ ref('link_dspace_metadatavalue_metadatafield') }}
    GROUP BY metadatafield_hk
)

SELECT
    d.scheme,
    d.element,
    d.qualifier,
    d.metadatafield_fullname,
    u.total_uses,
    u.metadatafield_hk
FROM usage_count u
LEFT JOIN {{ ref('dim_dspace_metadatafield') }} d USING (metadatafield_hk)
ORDER BY 1, 2, 3 DESC
