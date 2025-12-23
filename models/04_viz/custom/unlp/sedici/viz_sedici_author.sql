WITH authors AS (
    SELECT
        mv.text_value::text as text_value,
        mv.authority::text as authority,
        COUNT(*)::int as count
    FROM {{ref('er_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('er_dspace5_metadatavalue')}} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'sedici.creator.person'
    GROUP BY 1, 2
    ORDER BY count(*) DESC
)

SELECT * FROM authors
