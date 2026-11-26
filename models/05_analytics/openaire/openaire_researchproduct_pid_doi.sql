-- ¿Qué cobertura de DOI normalizados expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        doi_count,
        has_doi,
        has_single_doi
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
