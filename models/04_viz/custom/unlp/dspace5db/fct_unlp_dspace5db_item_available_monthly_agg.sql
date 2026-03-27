{{ config(materialized='table') }}

WITH monthly_counts AS (
    SELECT
        DATE_TRUNC('month', dc_date_available)::date AS available_month,
        COUNT(*) AS items_available_monthly
    FROM {{ ref('fct_unlp_dspace5db_item_publication') }}
    WHERE dc_date_available IS NOT NULL
    GROUP BY 1
),

month_bounds AS (
    SELECT
        MIN(available_month) AS min_month,
        MAX(available_month) AS max_month
    FROM monthly_counts
),

month_spine AS (
    SELECT
        GENERATE_SERIES(min_month, max_month, INTERVAL '1 month')::date AS available_month
    FROM month_bounds
    WHERE min_month IS NOT NULL
),

final AS (
    SELECT
        spine.available_month,
        COALESCE(monthly.items_available_monthly, 0) AS items_available_monthly,
        SUM(COALESCE(monthly.items_available_monthly, 0)) OVER (
            ORDER BY spine.available_month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS items_available_cumulative
    FROM month_spine AS spine
    LEFT JOIN monthly_counts AS monthly
        USING (available_month)
)

SELECT * FROM final
