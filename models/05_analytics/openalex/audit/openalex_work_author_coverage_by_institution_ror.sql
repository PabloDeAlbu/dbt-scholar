{{ config(materialized='view') }}

-- ¿Qué works fueron recuperados mediante el filtro institutions.ror?
WITH extracted_work AS (
    SELECT DISTINCT
        work_hk,
        _filter_value AS institution_ror,
        _filter_value_label AS institution_display_name,
        extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }}
    WHERE _filter_param = 'institutions.ror'
),

-- ¿Qué works poseen al menos una autoría en el bridge de OpenAlex?
work_with_authorship AS (
    SELECT DISTINCT
        work_hk
    FROM {{ ref('brg_openalex_work_authorship') }}
),

-- ¿Qué cobertura de autorías presenta cada extracción por institución?
final AS (
    SELECT
        extracted_work.extract_datetime,
        extracted_work.institution_ror,
        extracted_work.institution_display_name,
        COUNT(DISTINCT extracted_work.work_hk) AS extracted_work_count,
        COUNT(DISTINCT extracted_work.work_hk)
            FILTER (WHERE work_with_authorship.work_hk IS NOT NULL) AS work_with_authors_count,
        COUNT(DISTINCT extracted_work.work_hk)
            FILTER (WHERE work_with_authorship.work_hk IS NULL) AS work_without_authors_count,
        ROUND(
            COUNT(DISTINCT extracted_work.work_hk)
                FILTER (WHERE work_with_authorship.work_hk IS NOT NULL)::numeric
            / NULLIF(COUNT(DISTINCT extracted_work.work_hk), 0),
            4
        ) AS author_coverage_ratio
    FROM extracted_work
    LEFT JOIN work_with_authorship
        USING (work_hk)
    GROUP BY
        extracted_work.extract_datetime,
        extracted_work.institution_ror,
        extracted_work.institution_display_name
)

SELECT *
FROM final
