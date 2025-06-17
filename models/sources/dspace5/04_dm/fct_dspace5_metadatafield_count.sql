{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        dim.metadatafield_fullname,
        dim.short_id,
        dim.element,
        dim.qualifier,
        COUNT(*)
    FROM {{ref('dim_dspace5_metadatafieldregistry')}} dim
    INNER JOIN {{ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_mf.metadatafield_hk = dim.metadatafield_hk
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3, 4

)

SELECT * FROM base