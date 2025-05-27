{{ config(materialized='table') }}

WITH base AS (
    SELECT
        hub_mfr.metadata_field_hk, 
        sat_msr.short_id,
        sat_mfr.element,
        sat_mfr.qualifier
    FROM {{ref('hub_dspace7_metadatafieldregistry')}} hub_mfr
    INNER JOIN {{ref('sat_dspace7_metadatafieldregistry')}} sat_mfr ON sat_mfr.metadata_field_hk = hub_mfr.metadata_field_hk
    INNER JOIN {{ref('link_dspace7_metadatafield2metadataschema')}} lnk_mfr_msr ON lnk_mfr_msr.metadata_field_hk = hub_mfr.metadata_field_hk
    INNER JOIN {{ref('sat_dspace7_metadataschemaregistry')}} sat_msr ON sat_msr.metadata_schema_hk = lnk_mfr_msr.metadata_schema_hk    
)

SELECT * FROM base