{{ config(materialized = 'table')}}

WITH lnk_col_mf AS (
    SELECT DISTINCT metadatavalue_hk, resource_hk AS bitstream_hk
    FROM {{ ref('tlink_dspace5_metadatavalue_resource') }}
    WHERE resource_type_id = 0
),
collection_metadata AS (
    SELECT
        sat_mv.text_value,
        sat_mv.text_lang,
        sat_mv.place,
        sat_mv.authority,
        sat_mv.confidence,
        lnk_col_mf.metadatavalue_hk,
        lnk_col_mf.bitstream_hk
    FROM lnk_col_mf
	    JOIN {{ latest_satellite(ref('sat_dspace5_metadatavalue'), 'metadatavalue_hk', order_column='_load_datetime') }} AS sat_mv USING (metadatavalue_hk)
),

col_mf AS (
    SELECT 
        mv.text_value,
        mv.text_lang,
        mv.place,
        mv.authority,
        mv.confidence,
        mf.short_id,
        mf.element,
        mf.qualifier,
        mv.bitstream_hk
    FROM collection_metadata mv
    JOIN {{ ref('link_dspace5_metadatavalue_metadatafield') }} USING (metadatavalue_hk)
    JOIN {{ ref('dim_dspace5_metadatafield') }} mf USING (metadatafield_hk)
)

SELECT * FROM col_mf
