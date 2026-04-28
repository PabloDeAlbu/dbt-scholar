WITH base AS (
    SELECT
        work.type,
        COUNT(*) AS works_count
    FROM {{ ref('fct_openalex_work_publication') }} work
    GROUP BY 1
),
with_totals AS (
    SELECT
        type,
        works_count,
        SUM(works_count) OVER () AS total_works
    FROM base
)

SELECT
    type,
    works_count AS total,
    ROUND(works_count * 1.0 / NULLIF(total_works, 0), 3) * 100 AS "% de total"
FROM with_totals
ORDER BY 3 DESC
