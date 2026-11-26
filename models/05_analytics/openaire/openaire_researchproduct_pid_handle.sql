-- ¿Qué cobertura de Handle expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        handle_count,
        has_handle,
        has_single_handle
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
