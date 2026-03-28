{{ config(materialized='table') }}

WITH base AS (
    SELECT
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        sat_mv.metadatavalue_hk
    FROM {{ ref('hub_dspacedb5_metadatavalue') }} hub_mv
    JOIN {{ latest_satellite(ref('sat_dspacedb5_metadatavalue'), 'metadatavalue_hk', order_column='load_datetime') }} AS sat_mv
        ON sat_mv.metadatavalue_hk = hub_mv.metadatavalue_hk
)

SELECT * FROM base
