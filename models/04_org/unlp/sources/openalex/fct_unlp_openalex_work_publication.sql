{%- set unlp_ror = 'https://ror.org/01tjs6929' -%}

-- ¿Qué works fueron recuperados mediante el ROR de la UNLP?
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

-- ¿Qué instituciones de OpenAlex están relacionadas con el ROR de la UNLP?
unlp_institution AS (
    SELECT
        relationship.institution_hk,
        institution.institution_id,
        institution.institution_display_name,
        relationship.ror AS institution_ror
    FROM {{ ref('brg_openalex_institution_ror') }} relationship
    INNER JOIN {{ ref('dim_openalex_institution') }} institution
        USING (institution_hk)
    WHERE relationship.ror = '{{ unlp_ror }}'
),

-- ¿Qué atributos expone el subconjunto de works recuperado para la UNLP?
final AS (
    SELECT
        unlp_institution.institution_hk,
        unlp_institution.institution_id,
        unlp_institution.institution_display_name,
        unlp_institution.institution_ror,
        unlp_extract.unlp_first_extract_datetime,
        unlp_extract.unlp_last_extract_datetime,
        fct.doi_count > 1 AS has_multiple_doi,
        fct.*
    FROM {{ ref('fct_openalex_work_publication') }} AS fct
    INNER JOIN unlp_extract USING (work_hk)
    CROSS JOIN unlp_institution
)

SELECT *
FROM final
