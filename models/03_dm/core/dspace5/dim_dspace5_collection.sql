{{ config(materialized = 'table') }}

WITH col AS (
    SELECT 
        hub_c.collection_id,
        dim_c.text_value as title,
        hub_c.collection_hk
    FROM {{ref('hub_dspace5_collection')}} hub_c
	INNER JOIN {{ref('dim_dspace5_collection_metadatavalue')}} dim_c ON 
		dim_c.collection_hk = hub_c.collection_hk AND dim_c.short_id = 'dc' AND dim_c.element = 'title' AND dim_c.qualifier is null
),

final AS (
    SELECT 
        col.*,
        hub_h.handle as handle
    FROM col
    INNER JOIN {{ref('link_dspace5_handle_resource')}} lnk_h_r ON
        lnk_h_r.resource_hk = col.collection_hk
    INNER JOIN {{ref('hub_dspace5_handle')}} hub_h ON
        hub_h.handle_hk = lnk_h_r.handle_hk
    INNER JOIN {{ latest_satellite(ref('sat_dspace5_handle'), 'handle_hk') }} AS sat_h ON
        sat_h.handle_hk = lnk_h_r.handle_hk
    WHERE sat_h.resource_type_id = 3
)

SELECT * FROM final
