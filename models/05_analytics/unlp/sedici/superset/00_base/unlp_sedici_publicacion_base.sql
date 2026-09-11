{{ config(materialized='view') }}

SELECT *
FROM {{ ref('fct_unlp_sedicidb_item_publication') }}
WHERE in_archive IS TRUE
  AND withdrawn IS FALSE
  AND discoverable IS TRUE
