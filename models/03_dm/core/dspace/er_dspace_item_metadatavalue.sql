{{ config(materialized='table') }}

WITH base AS (
    SELECT
        dim_mv.metadatafield_fullname,
        dim_mv.scheme,
        dim_mv.element,
        dim_mv.qualifier,
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        dim_mv.metadatafield_hk,
        fct_p.item_hk,
        lnk_mv_dso.metadatavalue_hk
    FROM {{ref('link_dspace_metadatavalue_dspaceobject')}} lnk_mv_dso
    INNER JOIN {{ref('er_dspace_item')}} fct_p ON
        fct_p.item_hk = lnk_mv_dso.dspaceobject_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_mf.metadatavalue_hk = lnk_mv_dso.metadatavalue_hk
    INNER JOIN {{ref('er_dspace_metadatafield')}} dim_mv ON
        dim_mv.metadatafield_hk = lnk_mv_mf.metadatafield_hk
    INNER JOIN {{ latest_satellite(ref('sat_dspace_metadatavalue'), 'metadatavalue_hk') }} AS sat_mv ON
        sat_mv.metadatavalue_hk = lnk_mv_dso.metadatavalue_hk
)

SELECT * FROM base
