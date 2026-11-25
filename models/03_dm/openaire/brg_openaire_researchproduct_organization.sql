{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre researchproducts y organizaciones conserva OpenAIRE?
WITH relationship AS (
    SELECT 
        researchproduct_hk,
        organization_hk,
        researchproduct_organization_hk
    FROM {{ref('link_openaire_researchproduct_organization')}} 
),

-- ¿Qué organizaciones están asociadas con cada researchproduct?
final AS (
    SELECT
        researchproduct_organization_hk,
        researchproduct_hk,
        organization_hk
    FROM relationship
)

SELECT *
FROM final
