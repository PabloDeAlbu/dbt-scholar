{%- set conicet_ror = 'https://ror.org/03cqe8w59' -%}

-- ¿Qué works fueron recuperados mediante el ROR de CONICET?
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

-- ¿Qué instituciones de OpenAlex están relacionadas con el ROR de CONICET?
conicet_institution AS (
    SELECT
        relationship.institution_hk,
        institution.institution_id,
        institution.institution_display_name,
        relationship.ror AS institution_ror
    FROM {{ ref('brg_openalex_institution_ror') }} relationship
    INNER JOIN {{ ref('dim_openalex_institution') }} institution
        USING (institution_hk)
    WHERE relationship.ror = '{{ conicet_ror }}'
),

-- ¿Qué atributos expone el subconjunto de works recuperado para CONICET?
final AS (
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

SELECT *
FROM final
