WITH base AS (
    SELECT 
        UPPER(REPLACE(hub_type.dc_type, 'info:ar-repo/semantics/','')) as dc_type,
        seed.label,
        seed.coar_uri,
        hub_type.dc_type_hk
    FROM {{ ref('hub_oai_type') }} hub_type
    INNER JOIN {{ref('seed_coar_resource_types2conicet_oai_dc_types')}} seed ON hub_type.dc_type = seed.record_type
    WHERE coar_uri != '#N/A' and dc_type like 'info:ar-repo%'
    ORDER BY 1
)

SELECT * FROM base
