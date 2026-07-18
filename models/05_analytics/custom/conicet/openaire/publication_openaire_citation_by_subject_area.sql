WITH base AS (
    SELECT
        COALESCE(NULLIF(BTRIM(subject_area), ''), 'SIN ÁREA TEMÁTICA') AS subject_area,
        openaire_citation_count
    FROM {{ ref('publication_openaire_enrichment') }}
    WHERE matched_by_unique_original_id
),

grouped AS (
    SELECT
        subject_area,
        COUNT(*) AS matched_publication_count,
        COUNT(*) FILTER (
            WHERE openaire_citation_count IS NOT NULL
        ) AS publication_with_citation_data_count,
        COUNT(*) FILTER (
            WHERE openaire_citation_count > 0
        ) AS cited_publication_count,
        COALESCE(SUM(openaire_citation_count), 0) AS openaire_citation_count,
        ROUND(
            AVG(openaire_citation_count),
            3
        ) AS openaire_average_citation_count
    FROM base
    GROUP BY subject_area
),

final AS (
    SELECT
        subject_area,
        matched_publication_count,
        publication_with_citation_data_count,
        cited_publication_count,
        openaire_citation_count,
        ROUND(
            100.0 * openaire_citation_count
            / NULLIF(SUM(openaire_citation_count) OVER (), 0),
            3
        ) AS openaire_citation_share_pct,
        openaire_average_citation_count
    FROM grouped
)

SELECT *
FROM final
ORDER BY openaire_citation_count DESC, subject_area
