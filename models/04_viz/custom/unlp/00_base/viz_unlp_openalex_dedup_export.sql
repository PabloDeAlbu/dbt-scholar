{{ config(materialized='view') }}

WITH author AS (
    SELECT
        brg.work_hk,
        STRING_AGG(DISTINCT dim_author.display_name::text, '|' ORDER BY dim_author.display_name::text) AS author
    FROM {{ ref('brg_openalex_work_author') }} brg
    INNER JOIN {{ ref('dim_openalex_author') }} dim_author USING (author_hk)
    GROUP BY 1
),

base AS (
    SELECT
        work_hk,
        work_id,
        title,
        type,
        publication_year,
        publication_date,
        doi
    FROM {{ ref('fct_unlp_openalex_work') }}
)

SELECT
    base.title,
    NULL::text AS subtitle,
    base.type,
    author.author,
    CASE
        WHEN base.publication_date IS NOT NULL THEN base.publication_date::text
        WHEN base.publication_year IS NOT NULL THEN EXTRACT(YEAR FROM base.publication_year)::int::text
    END AS date,
    NULLIF(base.doi, '-') AS doi,
    NULL::text AS isbn,
    NULL::text AS issn,
    NULL::text AS description,
    base.work_id AS source_id,
    'openalex'::text AS source_system,
    EXTRACT(YEAR FROM base.publication_year)::int AS publication_year,
    NULL::text AS institutional_uri
FROM base
LEFT JOIN author USING (work_hk)
