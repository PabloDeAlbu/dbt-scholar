-- ¿Qué cobertura de arXiv expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        arxiv_count,
        has_arxiv,
        has_single_arxiv
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
