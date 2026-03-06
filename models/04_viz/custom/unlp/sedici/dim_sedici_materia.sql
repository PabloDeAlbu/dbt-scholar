WITH materias AS (
    SELECT
        mv.text_value::text as text_value,
        COUNT(*)::int as count
    FROM {{ ref('fct_unlp_ir_item_publication') }} unlp
    INNER JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv USING (item_hk)
    INNER JOIN {{ ref('dim_dspace5_metadatavalue') }} mv ON 
        mv.metadatavalue_hk = bridge_i_mv.metadatavalue_hk
    WHERE
        bridge_i_mv.metadatafield_fullname = 'sedici.subject.materias'
        AND unlp.in_archive = TRUE
        AND unlp.withdrawn = FALSE
    GROUP BY mv.text_value
    ORDER BY count(*) DESC
)

SELECT * FROM materias
