WITH base AS (
    SELECT 
        hub_type.dc_type,
        seed.label,
        seed.coar_uri,
        hub_type.dc_type_hk
    FROM {{ ref('hub_oai_type') }} hub_type
    INNER JOIN {{ref('seed_resource_types_coar2record_types_conicet')}} seed ON hub_type.dc_type = seed.record_type
    WHERE coar_uri != '#N/A' and dc_type like 'info:%'
    ORDER BY 1
)

SELECT * FROM base
