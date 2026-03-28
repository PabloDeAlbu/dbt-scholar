{{ config(materialized='table') }}

WITH base AS (
    SELECT
        hub_mf.metadatafield_bk,
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier,
        CASE
            WHEN sat_mf.qualifier IS NOT NULL
                THEN sat_ms.short_id || '.' || sat_mf.element || '.' || sat_mf.qualifier
            ELSE sat_ms.short_id || '.' || sat_mf.element
        END AS metadatafield_fullname,
        sat_mf.metadatafield_hk
    FROM {{ ref('hub_dspacedb5_metadatafieldregistry') }} hub_mf
    JOIN {{ latest_satellite(ref('sat_dspacedb5_metadatafieldregistry'), 'metadatafield_hk', order_column='load_datetime') }} AS sat_mf
        USING (metadatafield_hk)
    JOIN {{ ref('link_dspacedb5_metadatafield_metadataschema') }} lnk_mf_ms USING (metadatafield_hk)
    JOIN {{ latest_satellite(ref('sat_dspacedb5_metadataschemaregistry'), 'metadataschema_hk', order_column='load_datetime') }} AS sat_ms
        USING (metadataschema_hk)
),
final AS (
    SELECT
        metadatafield_fullname,
        short_id,
        element,
        qualifier,
        metadatafield_hk
    FROM base
    GROUP BY 1,2,3,4,5
)

SELECT * FROM final
