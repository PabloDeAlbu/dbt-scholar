{{ config(materialized='table') }}

WITH id AS (
    SELECT
        i_mv.item_hk,
        mv.text_value
    FROM {{ref('er_dspace5_item_metadatavalue')}} i_mv
    JOIN {{ref('er_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
    WHERE
        i_mv.metadatafield_fullname = 'dc.identifier.uri' AND
        mv.text_value LIKE 'http://sedici.unlp.edu.ar/handle/10915%'
),

date_accessioned AS (
  SELECT 
    i_mv.item_hk,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
  FROM {{ref('er_dspace5_item_metadatavalue')}} i_mv
  JOIN {{ref('er_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
  WHERE i_mv.metadatafield_fullname = 'dc.date.accessioned'
  GROUP BY i_mv.item_hk
),

date_issued AS (
  SELECT 
    i_mv.item_hk,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
  FROM {{ref('er_dspace5_item_metadatavalue')}} i_mv
  JOIN {{ref('er_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
  WHERE i_mv.metadatafield_fullname = 'dc.date.issued'
  GROUP BY i_mv.item_hk
),

dc_type AS (
    SELECT
        i_mv.item_hk,
        mv.text_value
    FROM {{ref('er_dspace5_item_metadatavalue')}} i_mv
    JOIN {{ref('er_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
    WHERE i_mv.metadatafield_fullname = 'dc.type'
),

final as (
    SELECT 
        item.item_id,
        id.text_value as id,
        to_timestamp(date_accessioned.text_value,'YYYY-MM-DD') as date_accessioned,
        to_timestamp(date_accessioned.text_value,'YYYY-MM-DD') as date_issued,
        dc_type.text_value as type,
        in_archive,
        withdrawn,
        discoverable
    FROM {{ref('er_dspace5_item')}} item
    JOIN id USING (item_hk)
    JOIN dc_type USING (item_hk)
    JOIN date_accessioned USING (item_hk)
    JOIN date_issued USING (item_hk)
)

SELECT * FROM final
