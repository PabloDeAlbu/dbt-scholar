{{ config(materialized='table') }}

WITH base AS (
    SELECT
        hub_mfr.metadatafield_hk, 
        sat_msr.short_id,
        sat_mfr.element,
        sat_mfr.qualifier,
        sat_mv.text_value,
        sat_mv.authority,
        sat_mv.metadatavalue_hk
    FROM {{ref('hub_dspace_metadatafieldregistry')}} hub_mfr
    INNER JOIN {{ref('sat_dspace_metadatafieldregistry')}} sat_mfr ON sat_mfr.metadatafield_hk = hub_mfr.metadatafield_hk
    INNER JOIN {{ref('link_dspace_metadatafield_metadataschema')}} lnk_mfr_msr ON lnk_mfr_msr.metadatafield_hk = hub_mfr.metadatafield_hk
    INNER JOIN {{ref('sat_dspace_metadataschemaregistry')}} sat_msr ON sat_msr.metadataschema_hk = lnk_mfr_msr.metadataschema_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_metadatafield')}} lnk_mv_mfr ON lnk_mv_mfr.metadatafield_hk = hub_mfr.metadatafield_hk
    INNER JOIN {{ref('sat_dspace_metadatavalue')}} sat_mv ON sat_mv.metadatavalue_hk = lnk_mv_mfr.metadatavalue_hk
)

SELECT * FROM base