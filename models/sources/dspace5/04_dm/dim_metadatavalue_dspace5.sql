{{ config(materialized='table') }}

WITH base AS (
    SELECT
        sat_mv.text_value,
        sat_mv.authority,
        sat_mv.confidence,
        sat_mv.metadatavalue_hk
    FROM {{ref('hub_dspace5_metadatavalue')}} hub_mv
    INNER JOIN {{ref('sat_dspace5_metadatavalue')}} sat_mv ON sat_mv.metadatavalue_hk = hub_mv.metadatavalue_hk
)

SELECT * FROM base