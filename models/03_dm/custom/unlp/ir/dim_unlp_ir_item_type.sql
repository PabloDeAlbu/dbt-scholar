WITH
dc_type AS (
    SELECT DISTINCT
        mv.text_value
    FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.type'
),
subtype AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
    WHERE
        bridge_i_mv.metadatafield_fullname = 'sedici.subtype'
),


SELECT * FROM dc_type

