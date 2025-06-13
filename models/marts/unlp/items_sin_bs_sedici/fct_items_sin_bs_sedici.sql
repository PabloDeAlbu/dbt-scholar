{{ config(materialized='table') }}

WITH base as (
	SELECT 
		h.text_value as item_handle, 
		t.text_value as item_title, 
		origin.text_value as origin, 
		(uri.text_value IS NOT NULL) as has_sedici_identifier_uri,
		c.title as col_title, 
		c.handle as col_handle
	FROM {{ref('fct_item_dspace5')}} i
	INNER JOIN {{ref('dim_item_metadatavalue_dspace5')}} h ON 
		i.item_hk = h.item_hk AND h.short_id = 'dc' AND h.element = 'identifier' AND h.qualifier = 'uri'
	INNER JOIN {{ref('dim_item_metadatavalue_dspace5')}} t ON 
		i.item_hk = t.item_hk AND t.short_id = 'dc' AND t.element = 'title' AND t.qualifier is null
	LEFT JOIN {{ref('bridge_item_bitstream_dspace5')}} i_bs ON
		i.item_hk = i_bs.item_hk
	LEFT JOIN {{ref('dim_item_metadatavalue_dspace5')}} origin ON
		i.item_hk = origin.item_hk AND origin.short_id = 'mods' AND origin.element = 'originInfo' AND origin.qualifier = 'place'
	LEFT JOIN {{ref('dim_item_metadatavalue_dspace5')}} uri ON
		i.item_hk = uri.item_hk AND uri.short_id = 'sedici' AND uri.element = 'identifier' AND uri.qualifier = 'uri'
	INNER JOIN {{ref('dim_collection_dspace5')}} c ON
		i.owningcollection_hk = c.collection_hk
	WHERE i_bs.item_hk IS NULL AND h.text_value like 'http://sedici.unlp.edu.ar/handle/10915/%'
)

SELECT * FROM base