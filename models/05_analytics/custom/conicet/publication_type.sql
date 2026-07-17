WITH base AS (
    SELECT
        COALESCE(publication_type_label_es, 'SIN TIPO NORMALIZADO') AS publication_type,
        publication_type_uri
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

grouped AS (
    SELECT
        publication_type,
        publication_type_uri,
        COUNT(*) AS publication_count
    FROM base
    GROUP BY publication_type, publication_type_uri
),

final AS (
    SELECT
        publication_type,
        publication_type_uri,
        publication_count,
        ROUND(
            100.0 * publication_count / NULLIF(SUM(publication_count) OVER (), 0),
            3
        ) AS publication_share_pct
    FROM grouped
)

SELECT *
FROM final
ORDER BY publication_count DESC, publication_type
