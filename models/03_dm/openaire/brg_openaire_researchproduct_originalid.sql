{{ config(materialized = 'table') }}

-- ¿Qué identificadores provenientes de fuentes upstream relaciona OpenAIRE con cada researchproduct?
WITH original_identifier AS (
    SELECT
        link.researchproduct_hk,
        link.original_hk,
        link.researchproduct_originalid_hk,
        original.original_id
    FROM {{ ref('link_openaire_researchproduct_originalid') }} AS link
    INNER JOIN {{ ref('hub_openaire_originalid') }} AS original
        USING (original_hk)
),

-- ¿Qué valores de identificadores originales quedan disponibles para cada researchproduct?
final AS (
    SELECT
        researchproduct_originalid_hk,
        researchproduct_hk,
        original_hk,
        original_id
    FROM original_identifier
)

SELECT *
FROM final
