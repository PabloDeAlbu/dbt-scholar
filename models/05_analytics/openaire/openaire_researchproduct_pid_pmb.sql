-- ¿Qué cobertura de PMB expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        pmb_count,
        has_pmb,
        has_single_pmb
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
