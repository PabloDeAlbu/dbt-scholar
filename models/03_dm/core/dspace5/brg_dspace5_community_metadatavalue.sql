{{ config(materialized = 'table')}}

WITH mv_filtered AS (
    SELECT DISTINCT metadatavalue_hk, resource_hk AS community_hk
    FROM {{ ref('tlink_dspace5_metadatavalue_resource') }}
    WHERE resource_type_id = 4
),
com_metadata AS (
    SELECT
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        f.metadatavalue_hk,
        f.community_hk
    FROM mv_filtered f
	    JOIN {{ latest_satellite(ref('sat_dspace5_metadatavalue'), 'metadatavalue_hk', order_column='_load_datetime') }} AS sat_mv USING (metadatavalue_hk)
),

final AS (
    SELECT 
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        mf.short_id,
        mf.element,
        mf.qualifier,
        mf.metadatafield_fullname,
        mf.metadatafield_hk,
        mv.metadatavalue_hk,
        mv.community_hk
    FROM com_metadata mv
    JOIN {{ ref('link_dspace5_metadatavalue_metadatafield') }} USING (metadatavalue_hk)
    JOIN {{ ref('dim_dspace5_metadatafield') }} mf USING (metadatafield_hk)
    JOIN {{ ref('hub_dspace5_community') }} comm USING (community_hk)
)

SELECT * FROM final
