{{ config(materialized='view') }}

WITH base AS (
    SELECT
        _filter_param,
        _filter_value,
        COALESCE(_filter_value_label, _filter_value) AS filter_label,
        extract_datetime::date AS extract_date,
        COUNT(*) AS works_count,
        COUNT(DISTINCT work_hk) AS distinct_works_count,
        MIN(publication_year) AS min_publication_year,
        MAX(publication_year) AS max_publication_year
    FROM {{ ref('viz_openalex_work_extraction_seguimiento') }}
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM base
