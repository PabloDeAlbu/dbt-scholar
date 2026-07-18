WITH base AS (
    SELECT
        COALESCE(publication_type_label_es, 'SIN TIPO NORMALIZADO') AS publication_type,
        publication_type_uri,
        COALESCE(access_right_label_es, 'SIN CONDICIÓN DE ACCESO NORMALIZADA') AS access_right,
        access_right_uri
    FROM {{ ref('fct_conicet_oai_record_publication') }}
),

grouped AS (
    SELECT
        publication_type,
        publication_type_uri,
        access_right,
        access_right_uri,
        COUNT(*) AS publication_count
    FROM base
    GROUP BY publication_type, publication_type_uri, access_right, access_right_uri
),

final AS (
    SELECT
        publication_type,
        publication_type_uri,
        access_right,
        access_right_uri,
        publication_count,
        ROUND(
            100.0 * publication_count
            / NULLIF(SUM(publication_count) OVER (PARTITION BY publication_type_uri), 0),
            3
        ) AS publication_type_share_pct,
        ROUND(
            100.0 * publication_count / NULLIF(SUM(publication_count) OVER (), 0),
            3
        ) AS corpus_share_pct
    FROM grouped
)

SELECT *
FROM final
ORDER BY publication_type, publication_count DESC, access_right
