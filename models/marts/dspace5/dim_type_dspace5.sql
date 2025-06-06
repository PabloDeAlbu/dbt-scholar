WITH base AS (
    SELECT 
        *
    FROM {{ref('sat_dspace5_metadatafieldregistry')}} sat_mf
    INNER JOIN {{ref('link_dspace5_metadatafield_metadataschema')}} lnk_mf_ms ON
        lnk_mf_ms.metadatafield_hk = sat_mf.metadatafield_hk
    INNER JOIN {{ref('sat_dspace5_metadataschemaregistry')}} sat_ms ON
        sat_ms.metadataschema_hk = lnk_mf_ms.metadataschema_hk
    WHERE sat_ms.short_id = 'dc' AND sat_mf.element = 'type' AND sat_mf.qualifier is null
)

SELECT 
    *
FROM base
--INNER JOIN {{ref('link_dspace5_metadatavalue_resource')}}