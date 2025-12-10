WITH base AS (
    SELECT
        dim_work.type,
        COUNT(*) AS works_count
    FROM {{ ref('dim_openalex_work') }} dim_work
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
