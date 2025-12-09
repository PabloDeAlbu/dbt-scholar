{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        dim_mf.metadatafield_fullname,
        lnk_mv_r.resource_hk as item_hk,
        lnk_mv_r.metadatavalue_hk
    FROM {{ ref('tlink_dspace5_metadatavalue_resource') }} lnk_mv_r
    INNER JOIN {{ ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_r.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    INNER JOIN {{ ref('dim_dspace5_metadatafieldregistry') }} dim_mf ON 
        lnk_mv_mf.metadatafield_hk = dim_mf.metadatafield_hk
    WHERE lnk_mv_r.resource_type_id = 2
)

SELECT * FROM base
