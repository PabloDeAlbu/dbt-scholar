-- ¿Qué cobertura de PMID expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        pmid_count,
        has_pmid,
        has_single_pmid
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
