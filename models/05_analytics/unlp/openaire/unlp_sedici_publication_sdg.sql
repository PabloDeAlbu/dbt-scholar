-- ¿Qué publicaciones integran el universo vigente y visible de SEDICI?
WITH sedici_publication AS (
    SELECT
        item_id,
        handle,
        dc_identifier_uri,
        dc_title,
        dc_type,
        sedici_subtype,
        dc_date_issued,
        publication_year,
        dc_date_accessioned,
        owning_root_child_community_title,
        owning_community_title,
        owning_collection_title,
        owning_community_path_titles
    FROM {{ ref('unlp_sedici_publicacion_base') }}
),

-- ¿Qué cobertura y qué correspondencia unívoca con OpenAIRE posee cada publicación?
openaire_coverage AS (
    SELECT
        item_id,
        has_openaire_match,
        openaire_match_count,
        has_unique_openaire_match,
        researchproduct_hk,
        researchproduct_id,
        openaire_original_id,
        openaire_match_rule,
        openaire_is_unlp_ror_product
    FROM {{ ref('unlp_sedici_publication_openaire_coverage') }}
),

-- ¿Qué atributos descriptivos aporta el producto OpenAIRE resuelto?
openaire_publication AS (
    SELECT
        researchproduct_hk,
        publication_date,
        type,
        main_title,
        publisher,
        best_access_right,
        best_access_right_uri
    FROM {{ ref('fct_openaire_researchproduct_publication') }}
),

-- ¿Qué Objetivos de Desarrollo Sostenible tiene asignado cada producto OpenAIRE?
openaire_sdg AS (
    SELECT
        researchproduct_hk,
        subject_hk,
        subject_value AS sdg,
        COUNT(*) OVER (PARTITION BY researchproduct_hk)::int AS sdg_count
    FROM {{ ref('brg_openaire_researchproduct_subject_sdg') }}
),

-- ¿Qué cobertura OpenAIRE y ODS presenta cada publicación vigente de SEDICI?
final AS (
    SELECT
        sedici.item_id,
        sedici.handle,
        sedici.dc_identifier_uri,
        sedici.dc_title AS sedici_title,
        sedici.dc_type AS sedici_type,
        sedici.sedici_subtype,
        sedici.dc_date_issued AS sedici_publication_date,
        sedici.publication_year AS sedici_publication_year,
        sedici.dc_date_accessioned AS sedici_accessioned_date,
        sedici.owning_root_child_community_title,
        sedici.owning_community_title,
        sedici.owning_collection_title,
        sedici.owning_community_path_titles,
        coverage.has_openaire_match,
        coverage.openaire_match_count,
        coverage.has_unique_openaire_match,
        coverage.researchproduct_hk,
        coverage.researchproduct_id,
        coverage.openaire_original_id,
        coverage.openaire_match_rule,
        coverage.openaire_is_unlp_ror_product,
        openaire.publication_date AS openaire_publication_date,
        openaire.type AS openaire_type,
        openaire.main_title AS openaire_title,
        openaire.publisher AS openaire_publisher,
        openaire.best_access_right AS openaire_best_access_right,
        openaire.best_access_right_uri AS openaire_best_access_right_uri,
        sdg.subject_hk,
        sdg.sdg,
        COALESCE(sdg.sdg_count, 0) AS sdg_count,
        sdg.subject_hk IS NOT NULL AS has_sdg
    FROM sedici_publication AS sedici
    INNER JOIN openaire_coverage AS coverage
        USING (item_id)
    LEFT JOIN openaire_publication AS openaire
        USING (researchproduct_hk)
    LEFT JOIN openaire_sdg AS sdg
        USING (researchproduct_hk)
)

SELECT *
FROM final
