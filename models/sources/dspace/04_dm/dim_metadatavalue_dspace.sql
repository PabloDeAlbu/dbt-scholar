{{ config(materialized='table') }}

WITH base AS (
    SELECT
        hub_mfr.metadata_field_hk, 
        sat_msr.short_id,
        sat_mfr.element,
        sat_mfr.qualifier,
        sat.*
    FROM {{ref('hub_dspace_metadatafieldregistry')}} hub_mfr
    INNER JOIN {{ref('sat_dspace_metadatafieldregistry')}} sat_mfr ON sat_mfr.metadata_field_hk = hub_mfr.metadata_field_hk
    INNER JOIN {{ref('link_dspace_metadatafield_metadataschema')}} lnk_mfr_msr ON lnk_mfr_msr.metadata_field_hk = hub_mfr.metadata_field_hk
    INNER JOIN {{ref('sat_dspace_metadataschemaregistry')}} sat_msr ON sat_msr.metadata_schema_hk = lnk_mfr_msr.metadata_schema_hk
    INNER JOIN {{ref('link_dspace_metadatavalue_metadatafield')}} lnk_mv_mfr ON lnk_mv_mfr.metadata_field_hk = hub_mfr.metadata_field_hk
    INNER JOIN {{ref('sat_dspace_metadatavalue')}} sat_mv ON sat_mv.metadatavalue_hk = lnk_mv_mfr.metadatavalue_hk
)

SELECT * FROM base