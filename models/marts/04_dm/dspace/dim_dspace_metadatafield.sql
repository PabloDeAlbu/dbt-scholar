{{ config(materialized='table') }}

SELECT
    sat_ms.short_id AS scheme,
    sat_mf.element,
    sat_mf.qualifier,
    CASE
        WHEN sat_mf.qualifier IS NOT NULL
            THEN sat_ms.short_id || '.' || sat_mf.element || '.' || sat_mf.qualifier
        ELSE sat_ms.short_id || '.' || sat_mf.element
    END AS metadatafield_fullname,
    hub_mf.metadatafield_hk
FROM {{ ref('hub_dspace_metadatafieldregistry') }} hub_mf
INNER JOIN {{ ref('sat_dspace_metadatafieldregistry') }} sat_mf
    ON sat_mf.metadatafield_hk = hub_mf.metadatafield_hk
INNER JOIN {{ ref('link_dspace_metadatafield_metadataschema') }} lnk_mf_ms
    ON lnk_mf_ms.metadatafield_hk = hub_mf.metadatafield_hk
INNER JOIN {{ ref('sat_dspace_metadataschemaregistry') }} sat_ms
    ON sat_ms.metadataschema_hk = lnk_mf_ms.metadataschema_hk
ORDER BY 1, 2, 3 DESC