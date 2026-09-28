WITH base AS (
    SELECT
        matched_by_unique_original_id,
        openaire_citation_count
    FROM {{ ref('publication_openaire_enrichment') }}
),

grouped AS (
    SELECT
        matched_by_unique_original_id,
        COUNT(*) AS publication_count,
        COUNT(*) FILTER (
            WHERE openaire_citation_count IS NOT NULL
        ) AS publication_with_citation_count
    FROM base
    GROUP BY matched_by_unique_original_id
),

final AS (
    SELECT
        CASE
            WHEN matched_by_unique_original_id THEN 'CON CORRESPONDENCIA'
            ELSE 'SIN CORRESPONDENCIA'
        END AS match_status,
        publication_count,
        ROUND(
            100.0 * publication_count / NULLIF(SUM(publication_count) OVER (), 0),
            3
        ) AS publication_share_pct,
        publication_with_citation_count
    FROM grouped
)

SELECT *
FROM final
ORDER BY publication_count DESC
