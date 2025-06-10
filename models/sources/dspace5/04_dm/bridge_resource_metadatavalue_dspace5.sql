WITH base AS (
    SELECT 
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier
    FROM {{ref('link_dspace5_metadatafield_metadataschema')}} lnk_mf_ms
    INNER JOIN {{ref('sat_dspace5_metadataschemaregistry')}} sat_ms ON
        sat_ms.metadataschema_hk = lnk_mf_ms.metadataschema_hk
    INNER JOIN {{ref('sat_dspace5_metadatafieldregistry')}} sat_mf ON
        sat_mf.metadatafield_hk = lnk_mf_ms.metadatafield_hk
)

SELECT * FROM base
