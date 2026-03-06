{{ config(materialized='table') }}

WITH src AS (
  SELECT
    b.item_hk,
    mv.metadatavalue_hk,
    mv.text_value AS materia
  FROM {{ ref('fct_unlp_ir_item_publication') }} unlp
  JOIN {{ ref('brg_dspace5_item_metadatavalue') }} b USING (item_hk)
  JOIN {{ ref('dim_dspace5_metadatavalue') }} mv
    ON mv.metadatavalue_hk = b.metadatavalue_hk
  WHERE b.metadatafield_fullname = 'sedici.subject.materias'
    AND mv.text_value IS NOT NULL
    AND trim(mv.text_value) <> ''
    AND unlp.in_archive = TRUE
    AND unlp.withdrawn = FALSE
)
SELECT DISTINCT
  item_hk,
  metadatavalue_hk,
  materia
FROM src
