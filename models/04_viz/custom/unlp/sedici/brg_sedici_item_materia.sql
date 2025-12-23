{{ config(materialized='table') }}

WITH src AS (
  SELECT
    b.item_hk,
    mv.metadatavalue_hk,
    mv.text_value AS materia
  FROM {{ ref('er_dspace5_item_metadatavalue') }} b
  JOIN {{ ref('er_dspace5_metadatavalue') }} mv
    ON mv.metadatavalue_hk = b.metadatavalue_hk
  WHERE b.metadatafield_fullname = 'dc.subject'
    AND mv.text_value IS NOT NULL
    AND trim(mv.text_value) <> ''
)
SELECT DISTINCT
  item_hk,
  metadatavalue_hk,
  materia
FROM src
