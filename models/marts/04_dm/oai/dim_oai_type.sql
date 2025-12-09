WITH base AS (
    SELECT 
        hub_type.types,
        hub_type.type_hk
    FROM {{ ref('hub_oai_type') }} hub_type
    ORDER BY 1
)

SELECT * FROM base
