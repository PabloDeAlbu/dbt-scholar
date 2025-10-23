WITH openaire AS (
    SELECT
        extract(year FROM publication_date) AS publication_year
    FROM {{ ref('viz_openaire_researchproduct') }}
    WHERE publication_date IS NOT NULL
),

openalex AS (
    SELECT
        extract(year FROM publication_date) AS publication_year
    FROM {{ ref('viz_openalex_work') }}
    WHERE publication_date IS NOT NULL
),

aggregated AS (
    SELECT
        'openaire' AS source,
        publication_year,
        count(*) AS publications
    FROM openaire
    WHERE publication_year IS NOT NULL
    GROUP BY 1, 2

    UNION ALL

    SELECT
        'openalex' AS source,
        publication_year,
        count(*) AS publications
    FROM openalex
    WHERE publication_year IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    source,
    publication_year,
    publications
FROM aggregated
ORDER BY publication_year, source
