WITH base AS (
    SELECT
        record_id,
        title,
        publication_date,
        publication_type,
        subject_area,
        subject_subarea,
        institutional_uri,
        doi,
        openaire_researchproduct_id,
        openaire_citation_count
    FROM {{ ref('publication_openaire_enrichment') }}
    WHERE matched_by_unique_original_id
      AND openaire_citation_count IS NOT NULL
),

ranked AS (
    SELECT
        ROW_NUMBER() OVER (
            ORDER BY openaire_citation_count DESC, record_id
        ) AS citation_rank,
        *
    FROM base
),

final AS (
    SELECT *
    FROM ranked
    WHERE citation_rank <= 10
)

SELECT *
FROM final
ORDER BY citation_rank
