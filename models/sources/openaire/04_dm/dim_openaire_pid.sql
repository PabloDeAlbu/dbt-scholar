{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        *
    FROM {{ref('hub_openaire_pid')}} hub_pid
)

SELECT * FROM base