{{ config(materialized='table') }}

WITH base as (
	SELECT 
		handle.text_value as item_handle,
		type.text_value as type,
		subtype.text_value as subtype,
		title.text_value as item_title,
		origin.text_value as origin,
		(uri.text_value IS NOT NULL) as has_sedici_identifier_uri,
		(loc.text_value IS NOT NULL) as has_mods_location,
		c.handle as col_handle,
		c.title as col_title,
		mv_com.text_value as com_title
	FROM {{ref('fct_dspace5_item')}} i

	INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_dc_identifier_uri ON 
		bridge_dc_identifier_uri.item_hk = i.item_hk AND 
		bridge_dc_identifier_uri.metadatafield_fullname = 'dc.identifier.uri'
	INNER JOIN {{ref('dim_dspace5_metadatavalue')}} handle ON 
		handle.metadatavalue_hk = bridge_dc_identifier_uri.metadatavalue_hk

	INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_dc_title ON 
		bridge_dc_title.item_hk = i.item_hk AND 
		bridge_dc_title.metadatafield_fullname = 'dc.title'
	INNER JOIN {{ref('dim_dspace5_metadatavalue')}} title ON 
		title.metadatavalue_hk = bridge_dc_title.metadatavalue_hk

	INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_dc_type ON 
		bridge_dc_type.item_hk = i.item_hk AND 
		bridge_dc_type.metadatafield_fullname = 'dc.type'
	INNER JOIN {{ref('dim_dspace5_metadatavalue')}} type ON 
		type.metadatavalue_hk = bridge_dc_type.metadatavalue_hk

	INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_sedici_subtype ON 
		bridge_sedici_subtype.item_hk = i.item_hk AND 
		bridge_sedici_subtype.metadatafield_fullname = 'sedici.subtype'
	INNER JOIN {{ref('dim_dspace5_metadatavalue')}} subtype ON 
		subtype.metadatavalue_hk = bridge_sedici_subtype.metadatavalue_hk

	LEFT JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_mods_origininfo_place ON 
		bridge_mods_origininfo_place.item_hk = i.item_hk AND 
		bridge_mods_origininfo_place.metadatafield_fullname = 'mods.originInfo.place'
	LEFT JOIN {{ref('dim_dspace5_metadatavalue')}} origin ON 
		origin.metadatavalue_hk = bridge_mods_origininfo_place.metadatavalue_hk

	LEFT JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_sedici_identifier_uri ON 
		bridge_sedici_identifier_uri.item_hk = i.item_hk AND 
		bridge_sedici_identifier_uri.metadatafield_fullname = 'sedici.identifier.uri'
	LEFT JOIN {{ref('dim_dspace5_metadatavalue')}} uri ON 
		uri.metadatavalue_hk = bridge_dc_title.metadatavalue_hk

	LEFT JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_mods_location ON 
		bridge_mods_location.item_hk = i.item_hk AND 
		bridge_mods_location.metadatafield_fullname = 'mods.location'
	LEFT JOIN {{ref('dim_dspace5_metadatavalue')}} loc ON 
		loc.metadatavalue_hk = bridge_dc_title.metadatavalue_hk

	INNER JOIN {{ref('dim_dspace5_collection')}} c ON
		i.owningcollection_hk = c.collection_hk

	LEFT JOIN {{ref('brg_dspace5_item_bitstream')}} i_bs ON
		i.item_hk = i_bs.item_hk

	INNER JOIN {{ ref('link_dspace5_community_collection')}} lnk_com_col ON
		lnk_com_col.collection_hk = c.collection_hk

	INNER JOIN {{ ref('brg_dspace5_community_metadatavalue') }} brd_com_mv ON 
		brd_com_mv.community_hk = lnk_com_col.community_hk AND brd_com_mv.metadatafield_fullname = 'dc.title'

	INNER JOIN {{ ref('dim_dspace5_metadatavalue')}} mv_com ON
		brd_com_mv.metadatavalue_hk = mv_com.metadatavalue_hk

	WHERE i_bs.item_hk IS NULL AND handle.text_value like 'http://sedici.unlp.edu.ar/handle/10915/%'
)

SELECT * FROM base