{{ config(materialized = 'table') }}

WITH base AS (
    SELECT 
        *
    FROM {{ref('hub_openaire_organization')}} hub_organization
    {# JOIN {{ref('sat_openaire_organization')} } #}
)

SELECT * FROM base