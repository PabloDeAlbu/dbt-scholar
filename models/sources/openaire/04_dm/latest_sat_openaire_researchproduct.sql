{{ config(materialized = 'table') }}

WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY researchproduct_hk
           ORDER BY load_datetime DESC  -- también equivale a src_eff en tu caso
         ) AS rn
  FROM {{ ref('sat_openaire_researchproduct') }}
)
SELECT *
FROM ranked
WHERE rn = 1