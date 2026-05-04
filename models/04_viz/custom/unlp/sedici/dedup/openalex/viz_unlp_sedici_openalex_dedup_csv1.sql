{{ config(materialized='view') }}

WITH base AS (
    SELECT
        work_hk,
        work_id,
        title,
        authors,
        publication_year,
        publication_date,
        doi,
        type
    FROM {{ ref('fct_unlp_openalex_work_publication') }}
    WHERE EXTRACT(YEAR FROM publication_year) = 2026
)

SELECT
    base.title,
    base.work_id AS id,
    base.authors AS author,
    base.publication_date,
    CASE
        WHEN base.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM base.publication_year)::int::text
    END AS publication_year,
    base.doi,
    base.type
FROM base
