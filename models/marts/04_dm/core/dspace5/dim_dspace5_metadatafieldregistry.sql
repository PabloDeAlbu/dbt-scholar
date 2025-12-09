{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        hub_mf.metadata_field_id,
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier,
        CASE
            WHEN sat_mf.qualifier IS NOT NULL
                THEN sat_ms.short_id || '.' || sat_mf.element || '.' || sat_mf.qualifier
            ELSE sat_ms.short_id || '.' || sat_mf.element
        END AS metadatafield_fullname,
        sat_mf.metadatafield_hk
    FROM {{ ref('hub_dspace5_metadatafieldregistry') }} hub_mf
    INNER JOIN {{ latest_satellite(ref('sat_dspace5_metadatafieldregistry'), 'metadatafield_hk') }} AS sat_mf ON
        hub_mf.metadatafield_hk = sat_mf.metadatafield_hk
    INNER JOIN {{ ref('link_dspace5_metadatafield_metadataschema') }} lnk_mf_ms ON
        sat_mf.metadatafield_hk = lnk_mf_ms.metadatafield_hk
    INNER JOIN {{ latest_satellite(ref('sat_dspace5_metadataschemaregistry'), 'metadataschema_hk') }} AS sat_ms ON
        lnk_mf_ms.metadataschema_hk = sat_ms.metadataschema_hk
)

SELECT * FROM base
