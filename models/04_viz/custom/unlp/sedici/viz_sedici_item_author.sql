{{ config(materialized='table') }}

WITH base AS (
  SELECT
    author.metadatavalue_hk,
    author.authority,
    item.item_hk
  FROM {{ ref('viz_sedici_author') }} author
  INNER JOIN {{ ref('brg_dspace5_item_metadatavalue') }} bridge_i_mv USING (metadatavalue_hk)
  INNER JOIN {{ ref('fct_unlp_ir_item_publication') }} item USING (item_hk)
  WHERE item.in_archive = TRUE AND item.withdrawn = FALSE
)

SELECT * FROM base
