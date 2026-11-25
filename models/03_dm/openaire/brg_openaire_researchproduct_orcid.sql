-- ¿Qué relaciones entre researchproducts e identificadores ORCID conserva OpenAIRE?
WITH relationship AS (
    SELECT
        researchproduct_orcid_hk,
        researchproduct_hk,
        orcid_hk
    FROM {{ref('link_openaire_researchproduct_orcid')}}
),

-- ¿Qué identificadores ORCID están asociados con cada researchproduct?
final AS (
    SELECT
        researchproduct_orcid_hk,
        researchproduct_hk,
        orcid_hk
    FROM relationship
)

SELECT *
FROM final
