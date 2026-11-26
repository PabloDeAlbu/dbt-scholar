-- ¿Qué cobertura de MAG expone cada researchproduct de OpenAIRE?
WITH final AS (
    SELECT
        researchproduct_id,
        researchproduct_hk,
        mag_count,
        has_mag,
        has_single_mag
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
)

SELECT *
FROM final
