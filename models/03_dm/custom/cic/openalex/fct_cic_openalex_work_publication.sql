{%- set cic_ror = 'https://ror.org/02s7sax82' -%}

WITH cic_extract AS (
    SELECT
        work_hk,
        MIN(extract_datetime) AS cic_first_extract_datetime,
        MAX(extract_datetime) AS cic_last_extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }}
    WHERE _filter_param = 'institutions.ror'
      AND _filter_value = '{{ cic_ror }}'
    GROUP BY work_hk
),

cic_institution AS (
    SELECT
        institution_hk,
        institution_id,
        institution_display_name,
        ror AS institution_ror
    FROM {{ ref('dim_openalex_institution') }}
    WHERE ror = '{{ cic_ror }}'
),

base AS (
    SELECT
        cic_institution.institution_hk,
        cic_institution.institution_id,
        cic_institution.institution_display_name,
        cic_institution.institution_ror,
        cic_extract.cic_first_extract_datetime,
        cic_extract.cic_last_extract_datetime,
        fct.*
    FROM {{ ref('fct_openalex_work_publication') }} fct
    INNER JOIN cic_extract USING (work_hk)
    CROSS JOIN cic_institution
)

SELECT * FROM base
