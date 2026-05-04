{{ config(materialized='view') }}

WITH extracted AS (
    SELECT DISTINCT
        extract.work_hk,
        extract._filter_value AS institution_ror,
        extract.extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }} extract
    WHERE extract._filter_param = 'institutions.ror'
),

author_presence AS (
    SELECT DISTINCT work_hk
    FROM {{ ref('brg_openalex_work_author') }}
),

base AS (
    SELECT
        extracted.extract_datetime,
        extracted.institution_ror,
        dim_i.institution_id,
        dim_i.institution_display_name,
        extracted.work_hk,
        (author_presence.work_hk IS NOT NULL) AS has_authors
    FROM extracted
    LEFT JOIN author_presence
        USING (work_hk)
    LEFT JOIN {{ ref('dim_openalex_institution') }} dim_i
        ON extracted.institution_ror = dim_i.ror
)

SELECT
    extract_datetime,
    institution_id,
    institution_ror,
    institution_display_name,
    COUNT(DISTINCT work_hk) AS extracted_work_count,
    COUNT(DISTINCT work_hk) FILTER (WHERE has_authors) AS work_with_authors_count,
    COUNT(DISTINCT work_hk) FILTER (WHERE NOT has_authors) AS work_without_authors_count,
    ROUND(
        COUNT(DISTINCT work_hk) FILTER (WHERE has_authors)::numeric
        / NULLIF(COUNT(DISTINCT work_hk), 0),
        4
    ) AS author_coverage_ratio
FROM base
GROUP BY
    extract_datetime,
    institution_id,
    institution_ror,
    institution_display_name
ORDER BY extract_datetime DESC, institution_ror
