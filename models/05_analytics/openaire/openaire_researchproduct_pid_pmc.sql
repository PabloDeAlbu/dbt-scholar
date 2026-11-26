-- ¿Qué cobertura de PMC expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        pmc_count,
        has_pmc,
        has_single_pmc
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
