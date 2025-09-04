{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        value,
        scheme,
        pid_hk
    FROM {{ref('hub_openaire_pid')}} hub_pid
)

SELECT * FROM base