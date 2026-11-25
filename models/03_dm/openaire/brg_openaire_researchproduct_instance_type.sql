{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre researchproducts y tipos de instancia conserva OpenAIRE?
WITH relationship AS (
    SELECT
        researchproduct_instances_type_hk AS researchproduct_instance_type_relationship_hk,
        researchproduct_hk,
        instancetype_hk AS researchproduct_instance_type_hk
    FROM {{ ref('link_openaire_researchproduct_instancetype') }}
),

-- ¿Qué tipos de instancia fueron observados para cada researchproduct?
final AS (
    SELECT
        researchproduct_instance_type_relationship_hk,
        researchproduct_hk,
        researchproduct_instance_type_hk
    FROM relationship
)

SELECT *
FROM final
