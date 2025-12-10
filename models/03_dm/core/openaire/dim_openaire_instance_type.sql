WITH base AS (
    SELECT 
        map.type,
        map.instance_type,
        instancetype_hk
    FROM {{ref('hub_openaire_instancetype')}} hub
    LEFT JOIN {{ref('map_openaire_type2instancetype')}} map ON map.instance_type = hub.type
)

SELECT * FROM base
