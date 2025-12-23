{{ config(materialized = 'table') }}

WITH col AS (
    SELECT 
        hub_c.collection_id,
        dim_c.text_value AS title,
        hub_c.collection_hk,
        ROW_NUMBER() OVER (
            PARTITION BY hub_c.collection_hk
            ORDER BY (dim_c.text_lang IS NULL) DESC, dim_c.text_lang, dim_c.place, dim_c.text_value
        ) AS rn_title
    FROM {{ ref('hub_dspace5_collection') }} hub_c
    JOIN {{ ref('er_dspace5_collection_metadatavalue') }} dim_c ON 
        dim_c.collection_hk = hub_c.collection_hk
        AND dim_c.short_id = 'dc'
        AND dim_c.element = 'title'
        AND dim_c.qualifier IS NULL
),

col_dedup AS (
    SELECT collection_id, title, collection_hk
    FROM col
    WHERE rn_title = 1
),

final AS (
    SELECT 
        col_dedup.*,
        hub_h.handle AS handle
    FROM col_dedup
    JOIN {{ ref('link_dspace5_handle_resource') }} lnk_h_r ON
        lnk_h_r.resource_hk = col_dedup.collection_hk
    JOIN {{ ref('hub_dspace5_handle') }} hub_h USING (handle_hk)
    JOIN {{ latest_satellite(ref('sat_dspace5_handle'), 'handle_hk') }} AS sat_h USING (handle_hk)
    WHERE sat_h.resource_type_id = 3
)

SELECT * FROM final
