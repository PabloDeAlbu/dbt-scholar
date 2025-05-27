WITH base as (
    SELECT
        TO_CHAR(dateavailable, 'YYYY-MM') AS year_month,
        COUNT(*) AS monthly_count,
        SUM(COUNT(*)) OVER (ORDER BY TO_CHAR(dateavailable, 'YYYY-MM')) AS cumulative_count
    FROM {{ref('fact_publication_dspace5')}} hub_item
    WHERE dateavailable IS NOT NULL
    GROUP BY TO_CHAR(dateavailable, 'YYYY-MM')
    ORDER BY year_month desc
    )

SELECT * FROM base