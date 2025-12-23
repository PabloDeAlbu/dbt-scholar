{{ config(materialized = 'table') }}

WITH final AS (
    SELECT 
        hub_b.bitstream_id,
        b_mv.text_value as title,
        hub_b.bitstream_hk
    FROM {{ref('hub_dspace5_bitstream')}} hub_b
	LEFT JOIN {{ref('er_dspace5_bitstream_metadatavalue')}} b_mv ON 
		b_mv.bitstream_hk = hub_b.bitstream_hk AND b_mv.short_id = 'dc' AND b_mv.element = 'title' AND b_mv.qualifier is null
)

SELECT * FROM final
