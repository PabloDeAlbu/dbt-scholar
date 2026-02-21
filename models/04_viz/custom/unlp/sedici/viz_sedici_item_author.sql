{{ config(materialized='table') }}

WITH base AS (
  SELECT
    author.metadatavalue_hk,
    author.authority,
    item.item_hk
  FROM {{ ref('viz_sedici_author') }} author
  INNER JOIN {{ref('er_dspace5_item_metadatavalue')}} bridge_i_mv USING (metadatavalue_hk)
  INNER JOIN {{ref('fct_dspace5_item')}}item USING (item_hk)
)

SELECT * FROM base