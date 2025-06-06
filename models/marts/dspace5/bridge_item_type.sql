WITH mf_dc_date_accessioned AS (
    SELECT 
        lnk_mf_ms.metadatafield_hk,
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier
    FROM {{ref('sat_dspace5_metadataschemaregistry')}} sat_ms
    INNER JOIN {{ref('link_dspace5_metadatafield_metadataschema')}} lnk_mf_ms ON
        lnk_mf_ms.metadataschema_hk = sat_ms.metadataschema_hk
    INNER JOIN {{ref('sat_dspace5_metadatafieldregistry')}} sat_mf ON
        sat_mf.metadatafield_hk = lnk_mf_ms.metadatafield_hk
    WHERE sat_ms.short_id = 'dc' AND sat_mf.element = 'type' AND sat_mf.qualifier is null
)

SELECT 
    lnk_mv_mf.metadatafield_hk,
    lnk_mv_mf.metadatavalue_hk
FROM mf_dc_date_accessioned
INNER JOIN {{ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf ON
lnk_mv_mf.metadatafield_hk = mf_dc_date_accessioned.metadatafield_hk