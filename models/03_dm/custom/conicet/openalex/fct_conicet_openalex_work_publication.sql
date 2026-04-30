{%- set conicet_ror = 'https://ror.org/03cqe8w59' -%}

WITH conicet_extract AS (
    SELECT
        work_hk,
        MIN(extract_datetime) AS conicet_first_extract_datetime,
        MAX(extract_datetime) AS conicet_last_extract_datetime
    FROM {{ ref('fct_openalex_work_extraction') }}
    WHERE _filter_param = 'institutions.ror'
      AND _filter_value = '{{ conicet_ror }}'
    GROUP BY work_hk
),

conicet_institution AS (
    SELECT
        institution_hk,
        institution_id,
        institution_display_name,
        ror AS institution_ror
    FROM {{ ref('dim_openalex_institution') }}
    WHERE ror = '{{ conicet_ror }}'
),

base AS (
    SELECT
        conicet_institution.institution_hk,
        conicet_institution.institution_id,
        conicet_institution.institution_display_name,
        conicet_institution.institution_ror,
        conicet_extract.conicet_first_extract_datetime,
        conicet_extract.conicet_last_extract_datetime,
        fct.*
    FROM {{ ref('fct_openalex_work_publication') }} fct
    INNER JOIN conicet_extract USING (work_hk)
    CROSS JOIN conicet_institution
)

SELECT * FROM base
