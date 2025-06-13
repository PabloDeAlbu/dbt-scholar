{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        dim_mf.metadatafield_fullname,
        lnk_mv_r.resource_hk as community_hk,
        lnk_mv_r.metadatavalue_hk
    FROM {{ ref('tlink_dspace5_metadatavalue_resource') }} lnk_mv_r
    INNER JOIN {{ ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_r.metadatavalue_hk = lnk_mv_mf.metadatavalue_hk
    INNER JOIN {{ ref('dim_metadatafieldregistry_dspace5') }} dim_mf ON 
        lnk_mv_mf.metadatafield_hk = dim_mf.metadatafield_hk
    WHERE lnk_mv_r.resource_type_id = 4
)

SELECT * FROM base
