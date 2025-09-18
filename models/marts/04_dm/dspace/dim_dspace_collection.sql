{{ config(materialized='table') }}

WITH collection AS (
    SELECT
        sat_mv.text_value,
        hub_col.collection_hk
    FROM {{ref('hub_dspace_collection')}} hub_col
    INNER JOIN {{ref('link_dspace_metadatavalue_dspaceobject')}} lnk_mv_dso ON 
        lnk_mv_dso.dspaceobject_hk = hub_col.collection_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_metadatafield')}} lnk_mv_mf ON 
        lnk_mv_mf.metadatavalue_hk = lnk_mv_dso.metadatavalue_hk
    INNER JOIN {{ref('dim_dspace_metadatafield')}} dim_mf ON
        dim_mf.metadatafield_hk = lnk_mv_mf.metadatafield_hk
    INNER JOIN {{ref('sat_dspace_metadatavalue')}} sat_mv ON 
        sat_mv.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    WHERE dim_mf.metadatafield_fullname = 'dc.title'
),

com_1 AS (
    SELECT 
        sat_mv.text_value as com_title, 
        c.text_value,
        c.collection_hk
    FROM collection c
    INNER JOIN {{ref('link_dspace_community_collection')}} lnk_col_com ON 
        lnk_col_com.collection_hk = c.collection_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_dspaceobject')}} lnk_mv_dso ON 
        lnk_mv_dso.dspaceobject_hk = lnk_col_com.community_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_metadatafield')}} lnk_mv_mf ON 
        lnk_mv_mf.metadatavalue_hk = lnk_mv_dso.metadatavalue_hk
    INNER JOIN {{ref('dim_dspace_metadatafield')}} dim_mf ON
        dim_mf.metadatafield_hk = lnk_mv_mf.metadatafield_hk
    INNER JOIN {{ref('sat_dspace_metadatavalue')}} sat_mv ON 
        sat_mv.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    WHERE dim_mf.metadatafield_fullname = 'dc.title'
)

SELECT * FROM com_1