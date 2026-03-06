{{ config(materialized = 'table') }}

WITH base AS (
    SELECT
        hub_i.item_id,
        mf.metadatafield_fullname,
        mf.short_id,
        mf.element,
        mf.qualifier,
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        mf.metadatafield_hk,
        lnk_mv_r.resource_hk as item_hk,
        lnk_mv_r.metadatavalue_hk
    FROM {{ ref('hub_dspace5_item') }} hub_i
    JOIN {{ ref('tlink_dspace5_metadatavalue_resource') }} lnk_mv_r ON hub_i.item_hk = lnk_mv_r.resource_hk
    JOIN {{ ref('link_dspace5_metadatavalue_metadatafield')}} lnk_mv_mf USING (metadatavalue_hk)
    JOIN {{ ref('dim_dspace5_metadatafield') }} mf USING (metadatafield_hk)
	    JOIN {{ latest_satellite(ref('sat_dspace5_metadatavalue'), 'metadatavalue_hk', order_column='_load_datetime') }} AS sat_mv USING (metadatavalue_hk)
    WHERE lnk_mv_r.resource_type_id = 2
)

SELECT * FROM base
