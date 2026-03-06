WITH authors AS (
    SELECT
        mv.metadatavalue_hk,
        mv.text_value::text as text_value,
        mv.authority::text as authority,
        COUNT(*)::int as count
    FROM {{ ref('fct_unlp_ir_item_publication') }} unlp
    INNER JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv USING (item_hk)
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'sedici.creator.person'
        AND unlp.in_archive = TRUE
        AND unlp.withdrawn = FALSE
    GROUP BY 1, 2, 3
    ORDER BY count(*) DESC
)

SELECT * FROM authors
