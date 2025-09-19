WITH base AS (
    SELECT 
        *
    FROM {{ref('hub_openaire_instancetype')}}
)

SELECT * FROM base