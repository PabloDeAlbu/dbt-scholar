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
)

SELECT
    base.title,
    base.work_id AS id,
    base.authors AS author,
    CASE
        WHEN base.publication_date IS NOT NULL THEN base.publication_date::text
        WHEN base.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM base.publication_year)::int::text
    END AS date,
    base.doi,
    base.type
FROM base
