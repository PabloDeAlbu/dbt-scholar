{{ config(materialized='view') }}

-- ¿Qué works fueron recuperados mediante un filtro institutions.ror?
WITH extracted AS (
    SELECT DISTINCT
        extract.work_hk,
        extract._filter_value AS institution_ror,
        extract.institution_id,
        extract._filter_value_label AS institution_display_name,
        extract.extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }} extract
    WHERE extract._filter_param = 'institutions.ror'
),

-- ¿Qué works recuperados poseen al menos una autoría?
author_presence AS (
    SELECT DISTINCT work_hk
    FROM {{ ref('brg_openalex_work_authorship') }}
),

-- ¿Qué estado de cobertura de autorías presenta cada work recuperado?
base AS (
    SELECT
        extracted.extract_datetime,
        extracted.institution_ror,
        extracted.institution_id,
        extracted.institution_display_name,
        extracted.work_hk,
        (author_presence.work_hk IS NOT NULL) AS has_authors
    FROM extracted
    LEFT JOIN author_presence
        USING (work_hk)
),

-- ¿Qué cobertura de autorías presenta cada extracción por institución?
final AS (
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
)

SELECT *
FROM final
ORDER BY extract_datetime DESC, institution_ror
