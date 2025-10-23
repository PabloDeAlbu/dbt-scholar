{{ config(materialized='table') }}

WITH base AS (
  SELECT
    author.metadatavalue_hk,
    author.authority,
    item.item_hk
  FROM {{ ref('dim_sedici_author') }} author
  INNER JOIN {{ref('brg_dspace5_item_metadatavalue')}} bridge_i_mv USING (metadatavalue_hk)
  INNER JOIN {{ref('fct_sedici_item')}}item USING (item_hk)
)

SELECT * FROM base