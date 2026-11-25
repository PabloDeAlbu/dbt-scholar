{{ config(materialized = 'table') }}

-- ¿Qué researchproducts identifica OpenAIRE?
WITH researchproduct AS (
    SELECT 
        hub.researchproduct_id,
        hub.researchproduct_hk
    FROM {{ref('hub_openaire_researchproduct')}} hub
),

-- ¿Cuál es el contrato mínimo de identidad de un researchproduct de OpenAIRE?
final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk
    FROM researchproduct
)

SELECT *
FROM final
