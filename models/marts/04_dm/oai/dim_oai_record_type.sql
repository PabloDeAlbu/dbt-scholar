WITH base AS (
    SELECT 
        dim_type.types,
        seed.label,
        seed.coar_uri,
        dim_type.type_hk
    FROM {{ ref('dim_oai_type') }} dim_type
    INNER JOIN {{ref('seed_resource_types_coar2record_types_conicet')}} seed ON dim_type.types = seed.record_type
    WHERE coar_uri != '#N/A' and types like 'info:%'
    ORDER BY 1
)

SELECT * FROM base
