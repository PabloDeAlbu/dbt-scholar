{%- set cic_ror = 'https://ror.org/02s7sax82' -%}

-- ¿Qué works fueron recuperados mediante el ROR de CIC?
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

-- ¿Qué instituciones de OpenAlex están relacionadas con el ROR de CIC?
cic_institution AS (
    SELECT
        relationship.institution_hk,
        institution.institution_id,
        institution.institution_display_name,
        relationship.ror AS institution_ror
    FROM {{ ref('brg_openalex_institution_ror') }} relationship
    INNER JOIN {{ ref('dim_openalex_institution') }} institution
        USING (institution_hk)
    WHERE relationship.ror = '{{ cic_ror }}'
),

-- ¿Qué atributos expone el subconjunto de works recuperado para CIC?
final AS (
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

SELECT *
FROM final
