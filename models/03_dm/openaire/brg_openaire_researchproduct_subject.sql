{{ config(materialized = 'table') }}

-- ¿Qué relaciones entre researchproducts y subjects conserva OpenAIRE?
WITH relationship AS (
    SELECT 
        researchproduct_hk,
        subject_hk,
        researchproduct_subject_hk
    FROM {{ref('link_openaire_researchproduct_subject')}} 
),

-- ¿Qué subjects están asociados con cada researchproduct?
final AS (
    SELECT
        researchproduct_hk,
        subject_hk,
        researchproduct_subject_hk
    FROM relationship
)

SELECT *
FROM final
