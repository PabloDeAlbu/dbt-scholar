{{ config(materialized = 'table') }}

WITH base AS (
  SELECT *
  FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT * FROM base