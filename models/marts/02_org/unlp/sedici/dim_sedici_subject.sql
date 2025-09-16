WITH subject AS (
    SELECT
        mv.text_value::text as text_value,
        COUNT(*)::int as count
    FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.subject'
    GROUP BY mv.text_value
    ORDER BY count(*) DESC
)

SELECT * FROM subject