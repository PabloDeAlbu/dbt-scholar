{{ config(materialized='view') }}

-- ¿Qué publicaciones integran el universo vigente y visible de SEDICI?
WITH sedici_publication AS (
    SELECT item_id
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Qué cobertura unívoca aporta OpenAlex para cada publicación?
openalex_match AS (
    SELECT
        item_id,
        BOOL_OR(is_unique_match) AS has_unique_openalex_match,
        (ARRAY_AGG(work_hk) FILTER (WHERE is_unique_match))[1] AS work_hk
    FROM {{ ref('brg_unlp_sedicidb_openalex_publication') }}
    GROUP BY item_id
),

-- ¿Qué cobertura y qué correspondencia unívoca aporta OpenAIRE?
openaire_coverage AS (
    SELECT
        item_id,
        has_openaire_match,
        has_unique_openaire_match,
        researchproduct_hk
    FROM {{ ref('unlp_sedici_publication_openaire_coverage') }}
),

-- ¿Qué correspondencias externas se confirman entre OpenAlex y OpenAIRE?
external_pair AS (
    SELECT
        work_hk,
        researchproduct_hk,
        BOOL_OR(is_unique_match) AS is_unique_match
    FROM {{ ref('brg_unlp_openalex_openaire_publication') }}
    GROUP BY
        work_hk,
        researchproduct_hk
),

-- ¿Qué estado de cobertura externa presenta cada publicación?
coverage AS (
    SELECT
        sedici.item_id,
        openalex.item_id IS NOT NULL AS has_openalex_match,
        COALESCE(openalex.has_unique_openalex_match, FALSE)
            AS has_unique_openalex_match,
        openaire.has_openaire_match,
        openaire.has_unique_openaire_match,
        CASE
            WHEN NOT COALESCE(openalex.has_unique_openalex_match, FALSE)
                OR NOT openaire.has_unique_openaire_match
                THEN 'not_applicable'
            WHEN pair.work_hk IS NULL
                THEN 'not_confirmed_by_doi'
            WHEN pair.is_unique_match
                THEN 'confirmed_unique'
            ELSE 'confirmed_ambiguous'
        END AS external_pair_status
    FROM sedici_publication AS sedici
    LEFT JOIN openalex_match AS openalex
        USING (item_id)
    INNER JOIN openaire_coverage AS openaire
        USING (item_id)
    LEFT JOIN external_pair AS pair
        ON pair.work_hk = openalex.work_hk
       AND pair.researchproduct_hk = openaire.researchproduct_hk
),

-- ¿Qué categorías legibles expone el dashboard para cada publicación?
final AS (
    SELECT
        item_id,
        CASE
            WHEN has_openalex_match THEN 'Sí'
            ELSE 'No'
        END AS en_openalex,
        CASE
            WHEN has_unique_openalex_match THEN 'Unívoca'
            WHEN has_openalex_match THEN 'Ambigua'
            ELSE 'No encontrada'
        END AS correspondencia_openalex,
        CASE
            WHEN has_openaire_match THEN 'Sí'
            ELSE 'No'
        END AS en_openaire,
        CASE
            WHEN has_unique_openaire_match THEN 'Unívoca'
            WHEN has_openaire_match THEN 'Ambigua'
            ELSE 'No encontrada'
        END AS correspondencia_openaire,
        CASE external_pair_status
            WHEN 'confirmed_unique' THEN 'Confirmada de forma unívoca'
            WHEN 'confirmed_ambiguous' THEN 'Confirmada con ambigüedad'
            WHEN 'not_confirmed_by_doi' THEN 'No confirmada por DOI'
            ELSE 'No aplica'
        END AS confirmacion_entre_fuentes_externas
    FROM coverage
)

SELECT *
FROM final
