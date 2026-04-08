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
        openaire_researchproduct_id,
        openaire_publication_date,
        openaire_type,
        openaire_access_right,
        openaire_citation_count
    FROM base
)

SELECT * FROM final
