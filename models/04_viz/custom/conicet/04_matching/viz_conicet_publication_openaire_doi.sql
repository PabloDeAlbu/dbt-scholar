WITH base AS (
    SELECT
        record_hk,
        record_id,
        title,
        publication_date,
        publication_year,
        publication_type,
        access_right,
        institutional_uri,
        doi,
        matched_by_unique_doi,
        openaire_researchproduct_id,
        openaire_publication_date,
        openaire_type,
        openaire_access_right,
        openaire_citation_count
    FROM {{ ref('fct_conicet_publication') }}
),

final AS (
    SELECT
        record_hk,
        record_id,
        title,
        publication_date,
        publication_year,
        publication_type,
        access_right,
        institutional_uri,
        doi,
        matched_by_unique_doi,
        CASE
            WHEN doi IS NOT NULL AND TRIM(doi) <> '' THEN 'Con DOI'
            ELSE 'Sin DOI'
        END AS has_doi_label,
        CASE
            WHEN matched_by_unique_doi THEN 'Match DOI con OpenAIRE'
            ELSE 'Sin match DOI con OpenAIRE'
        END AS matched_by_unique_doi_label,
        CASE
            WHEN doi IS NULL OR TRIM(doi) = '' THEN 'Sin DOI'
            WHEN matched_by_unique_doi THEN 'Con DOI y con match'
            ELSE 'Con DOI y sin match'
        END AS doi_integration_status_label,
        openaire_researchproduct_id,
        openaire_publication_date,
        openaire_type,
        openaire_access_right,
        openaire_citation_count
    FROM base
)

SELECT * FROM final
