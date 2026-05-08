{%- set unlp_ror = 'https://ror.org/01tjs6929' -%}

WITH unlp_extract AS (
    SELECT
        work_hk,
        MIN(extract_datetime) AS unlp_first_extract_datetime,
        MAX(extract_datetime) AS unlp_last_extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }}
    WHERE _filter_param = 'institutions.ror'
      AND _filter_value = '{{ unlp_ror }}'
    GROUP BY work_hk
),

unlp_institution AS (
    SELECT
        institution_hk,
        institution_id,
        institution_display_name,
        ror AS institution_ror
    FROM {{ ref('dim_openalex_institution') }}
    WHERE ror = '{{ unlp_ror }}'
),

doi_stats AS (
    SELECT
        work_hk,
        COUNT(DISTINCT doi_hk) AS doi_count,
        (COUNT(DISTINCT doi_hk) > 1) AS has_multiple_doi
    FROM {{ ref('link_openalex_work_doi') }}
    GROUP BY work_hk
),

base AS (
    SELECT
        unlp_institution.institution_hk,
        unlp_institution.institution_id,
        unlp_institution.institution_display_name,
        unlp_institution.institution_ror,
        unlp_extract.unlp_first_extract_datetime,
        unlp_extract.unlp_last_extract_datetime,
        COALESCE(doi_stats.doi_count, 0) AS doi_count,
        COALESCE(doi_stats.has_multiple_doi, FALSE) AS has_multiple_doi,
        fct.*
    FROM {{ ref('fct_openalex_work_publication') }} AS fct
    INNER JOIN unlp_extract USING (work_hk)
    LEFT JOIN doi_stats USING (work_hk)
    CROSS JOIN unlp_institution
)

SELECT * FROM base
