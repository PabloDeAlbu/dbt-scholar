WITH base AS (
    SELECT
        EXTRACT(YEAR FROM date_issued)::integer AS publication_year,
        COALESCE(publication_type_label_es, 'SIN TIPO NORMALIZADO') AS publication_type,
        publication_type_uri
    FROM {{ ref('fct_conicet_oai_record_publication') }}
    WHERE valid_date_issued
),

grouped AS (
    SELECT
        publication_year,
        publication_type,
        publication_type_uri,
        COUNT(*) AS publication_count
    FROM base
    GROUP BY publication_year, publication_type, publication_type_uri
),

final AS (
    SELECT
        publication_year,
        publication_type,
        publication_type_uri,
        publication_count,
        SUM(publication_count) OVER (PARTITION BY publication_year) AS publication_year_count,
        ROUND(
            100.0 * publication_count
            / NULLIF(SUM(publication_count) OVER (PARTITION BY publication_year), 0),
            3
        ) AS publication_year_share_pct
    FROM grouped
)

SELECT *
FROM final
ORDER BY publication_year, publication_count DESC, publication_type
