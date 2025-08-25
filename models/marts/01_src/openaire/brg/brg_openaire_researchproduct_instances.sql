WITH base AS (
    SELECT 
        hub.type,
        lnk.researchproduct_hk,
        lnk.instances_type_hk
    FROM {{ref('link_openaire_researchproduct_instances')}} lnk
    INNER JOIN {{ref('hub_openaire_instances_type')}} hub ON lnk.instances_type_hk = hub.instances_type_hk

)

SELECT * FROM base