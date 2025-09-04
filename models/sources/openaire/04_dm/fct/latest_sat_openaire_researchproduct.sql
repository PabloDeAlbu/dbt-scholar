{{ config(materialized = 'table') }}

WITH ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY researchproduct_hk
      ORDER BY load_datetime DESC
    ) AS rn
  FROM {{ ref('sat_openaire_researchproduct') }}
)
SELECT *
FROM ranked
WHERE rn = 1