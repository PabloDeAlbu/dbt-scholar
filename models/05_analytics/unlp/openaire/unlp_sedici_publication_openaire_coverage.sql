-- ¿Qué publicaciones integran el universo vigente y visible de SEDICI?
WITH sedici_publication AS (
    SELECT item_id
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Qué correspondencia unívoca con OpenAIRE puede resolverse para cada publicación?
openaire_match AS (
    SELECT
        item_id,
        MAX(sedici_match_count)::int AS openaire_match_count,
        BOOL_OR(is_unique_match) AS has_unique_openaire_match,
        (ARRAY_AGG(researchproduct_hk) FILTER (WHERE is_unique_match))[1]
            AS researchproduct_hk,
        MIN(researchproduct_id) FILTER (WHERE is_unique_match) AS researchproduct_id,
        MIN(original_id) FILTER (WHERE is_unique_match) AS openaire_original_id,
        MIN(match_rule) FILTER (WHERE is_unique_match) AS openaire_match_rule,
        (ARRAY_AGG(is_unlp_ror_product) FILTER (WHERE is_unique_match))[1]
            AS openaire_is_unlp_ror_product
    FROM {{ ref('brg_unlp_sedicidb_openaire_publication') }}
    GROUP BY item_id
),

-- ¿Qué cobertura OpenAIRE y qué ambigüedad presenta cada publicación de SEDICI?
final AS (
    SELECT
        sedici.item_id,
        openaire.item_id IS NOT NULL AS has_openaire_match,
        COALESCE(openaire.openaire_match_count, 0) AS openaire_match_count,
        COALESCE(openaire.has_unique_openaire_match, FALSE)
            AS has_unique_openaire_match,
        openaire.researchproduct_hk,
        openaire.researchproduct_id,
        openaire.openaire_original_id,
        openaire.openaire_match_rule,
        openaire.openaire_is_unlp_ror_product
    FROM sedici_publication AS sedici
    LEFT JOIN openaire_match AS openaire
        USING (item_id)
)

SELECT *
FROM final
