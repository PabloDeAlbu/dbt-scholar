WITH base AS (
    SELECT 
        hub_relation.relations,
        hub_relation.relation_hk
    FROM {{ ref('hub_oai_relation') }} hub_relation
)

SELECT * FROM base
