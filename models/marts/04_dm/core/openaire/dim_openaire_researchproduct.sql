{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        hub.researchproduct_id,
        hub.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub
)

SELECT * FROM base