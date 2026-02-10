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
        id.text_value as id,
        to_timestamp(date_accessioned.text_value,'YYYY-MM-DD') as date_accessioned,
        dc_type.text_value as type
    FROM {{ref('er_dspace5_item')}} item
    INNER JOIN id USING (item_hk)
    INNER JOIN dc_type USING (item_hk)
    INNER JOIN date_accessioned USING (item_hk)
    WHERE in_archive = True AND withdrawn = False
)

SELECT * FROM final
