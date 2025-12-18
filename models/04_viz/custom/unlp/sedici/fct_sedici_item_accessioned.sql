{{ config(materialized='table') }}

WITH id AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.identifier.uri' AND
        mv.text_value LIKE 'http://sedici.unlp.edu.ar/handle/10915%'
),

date_accessioned AS (
  SELECT 
    bridge_i_mv.item_hk,
    STRING_AGG(mv.text_value, '|' ORDER BY mv.text_value) as text_value
  FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv
  JOIN {{ref('dim_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
  WHERE bridge_i_mv.metadatafield_fullname = 'dc.date.accessioned'
  GROUP BY bridge_i_mv.item_hk
),

dc_type AS (
    SELECT
        bridge_i_mv.item_hk,
        mv.text_value
    FROM {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv
    INNER JOIN {{ref('dim_dspace5_metadatavalue')}} mv USING (metadatavalue_hk)
    WHERE
        bridge_i_mv.metadatafield_fullname = 'dc.type'
),

final as (
    SELECT 
        id.text_value as id,
        to_timestamp(date_accessioned.text_value,'YYYY-MM-DD') as date_accessioned,
        dc_type.text_value as type
    FROM {{ref('fct_dspace5_item_publication')}} item
    INNER JOIN id USING (item_hk)
    INNER JOIN dc_type USING (item_hk)
    INNER JOIN date_accessioned USING (item_hk)
    WHERE in_archive = True AND withdrawn = False
)

SELECT * FROM final
