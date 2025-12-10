{{ config(materialized='table') }}

WITH base AS (
    SELECT
        dim_mv.metadatafield_fullname,
        fct_p.item_hk,
        lnk_mv_dso.metadatavalue_hk
    FROM {{ref('link_dspace_metadatavalue_dspaceobject')}} lnk_mv_dso
    INNER JOIN {{ref('fct_dspace_item')}} fct_p ON
        fct_p.item_hk = lnk_mv_dso.dspaceobject_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_mf.metadatavalue_hk = lnk_mv_dso.metadatavalue_hk
    INNER JOIN {{ref('dim_dspace_metadatafield')}} dim_mv ON
        dim_mv.metadatafield_hk = lnk_mv_mf.metadatafield_hk
)

SELECT * FROM base
