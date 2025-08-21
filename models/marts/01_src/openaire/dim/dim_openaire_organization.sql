{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        *
    FROM {{ref('hub_openaire_organization')}} hub_organization
)

SELECT * FROM base