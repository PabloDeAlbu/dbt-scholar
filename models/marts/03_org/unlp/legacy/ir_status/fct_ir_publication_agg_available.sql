{{ config(materialized='table') }}

WITH base as (
	SELECT
		EXTRACT(YEAR FROM d.text_value::timestamp) AS año,
		EXTRACT(MONTH FROM d.text_value::timestamp) AS mes,
		COUNT(*)
	FROM {{ref('fct_dspace5_item')}} i
	INNER JOIN {{ref('dim_dspace5_item_metadatavalue')}} d ON 
		i.item_hk = d.item_hk AND d.short_id = 'dc' AND d.element = 'date' AND d.qualifier = 'available'
    WHERE i.in_archive = True AND i.withdrawn = False
	GROUP BY 1, 2
	ORDER BY 1 desc, 2 desc
)

SELECT * FROM base