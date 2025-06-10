{{ config(materialized = 'table')}}

WITH item_metadata AS (
    SELECT
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        sat_mv.metadatavalue_hk,
        lnk_mv_r.resource_hk as item_hk
    FROM {{ref('sat_dspace5_metadatavalue')}} sat_mv
    INNER JOIN {{ref('link_dspace5_metadatavalue_resource')}} lnk_mv_r ON
        lnk_mv_r.metadatavalue_hk = sat_mv.metadatavalue_hk
    WHERE sat_mv.resource_type_id = 2
),

item_metadatafield AS (
    SELECT 
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier,
        mv.item_hk
    FROM item_metadata mv
    INNER JOIN {{ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_mf.metadatavalue_hk = mv.metadatavalue_hk
    INNER JOIN {{ref('sat_dspace5_metadatafieldregistry')}} sat_mf ON
        sat_mf.metadatafield_hk = lnk_mv_mf.metadatafield_hk
    INNER JOIN {{ref('link_dspace5_metadatafield_metadataschema')}} lnk_mf_ms ON
        lnk_mf_ms.metadatafield_hk = sat_mf.metadatafield_hk
    INNER JOIN {{ref('sat_dspace5_metadataschemaregistry')}} sat_ms ON
        sat_ms.metadataschema_hk = lnk_mf_ms.metadataschema_hk
)

SELECT * FROM item_metadatafield