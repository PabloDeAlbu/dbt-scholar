WITH subject AS (
    SELECT
        mv.text_value::text as text_value,
        COUNT(*)::int as count
    FROM {{ref('er_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('er_dspace5_metadatavalue')}} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.subject'
    GROUP BY mv.text_value
    ORDER BY count(*) DESC
)

SELECT * FROM subject