{{ config(materialized = 'table') }}

WITH base as (
    SELECT 
        hub_mf.metadata_field_id,
        sat_ms.short_id,
        sat_mf.element,
        sat_mf.qualifier,
        CASE
            WHEN sat_mf.qualifier IS NOT NULL
                THEN sat_ms.short_id || '.' || sat_mf.element || '.' || sat_mf.qualifier
            ELSE sat_ms.short_id || '.' || sat_mf.element
        END AS metadatafield_fullname,
        sat_mf.metadatafield_hk
    FROM {{ ref('hub_dspace5_metadatafieldregistry') }} hub_mf
    JOIN {{ latest_satellite(ref('sat_dspace5_metadatafieldregistry'), 'metadatafield_hk') }} AS sat_mf USING (metadatafield_hk)
    JOIN {{ ref('link_dspace5_metadatafield_metadataschema') }} lnk_mf_ms USING (metadatafield_hk)
    JOIN {{ latest_satellite(ref('sat_dspace5_metadataschemaregistry'), 'metadataschema_hk') }} AS sat_ms USING (metadataschema_hk)
),

final as (
    SELECT 
        base.metadatafield_fullname,
        base.short_id,
        base.element,
        base.qualifier,
        base.metadatafield_hk
    FROM base
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY 1, 2, 3, 4, 5

)

SELECT * FROM final
