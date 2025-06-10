{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        hub_mf.metadata_field_id,
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier,
        sat_mf.metadatafield_hk
    FROM {{ ref('hub_dspace5_metadatafieldregistry') }} hub_mf
    INNER JOIN {{ ref('sat_dspace5_metadatafieldregistry') }} sat_mf ON
        hub_mf.metadatafield_hk = sat_mf.metadatafield_hk
    INNER JOIN {{ ref('link_dspace5_metadatafield_metadataschema') }} lnk_mf_ms ON
        sat_mf.metadatafield_hk = lnk_mf_ms.metadatafield_hk
    INNER JOIN {{ ref('sat_dspace5_metadataschemaregistry' )}} sat_ms ON
        lnk_mf_ms.metadataschema_hk = sat_ms.metadataschema_hk
),

final as (
    SELECT 
        base.metadata_field_id,
        base.short_id,
        base.element,
        base.qualifier,
        base.metadatafield_hk,
        COUNT(*)
    FROM base
    INNER JOIN {{ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf ON
        lnk_mv_mf.metadatafield_hk = base.metadatafield_hk
    GROUP BY 1, 2, 3, 4, 5

)

SELECT * FROM final