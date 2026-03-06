{{ config(materialized='table') }}

SELECT
    item_hk,
    dc_identifier_uri AS id,
    date_accessioned,
    type
FROM {{ ref('fct_unlp_ir_item_publication') }}
WHERE in_archive = TRUE
  AND withdrawn = FALSE
  AND date_accessioned IS NOT NULL
